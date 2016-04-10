/*
 * Copyright (C) 2015 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.cay.services.em.evaluator.impl;

import edu.snu.cay.services.em.avro.DataOpType;
import edu.snu.cay.services.em.avro.UnitIdPair;
import edu.snu.cay.services.em.evaluator.api.RemoteAccessibleMemoryStore;
import edu.snu.cay.services.em.msg.api.ElasticMemoryMsgSender;
import edu.snu.cay.services.em.serialize.Serializer;
import edu.snu.cay.utils.LongRangeUtils;
import edu.snu.cay.utils.Tuple3;
import edu.snu.cay.utils.trace.HTrace;
import org.apache.commons.lang.math.LongRange;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.util.Optional;
import org.htrace.Trace;
import org.htrace.TraceInfo;
import org.htrace.TraceScope;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A {@code MemoryStore} implementation based on {@code TreeMap}s inside a single {@code HashMap}.
 * All data of one data type is stored in a {@code TreeMap}, ordered by data ids.
 * These {@code TreeMap}s are then maintained as values of one big {@code HashMap}, which uses the data types as keys.
 * A {@code ReentrantReadWriteLock} is used for synchronization between {@code get}, {@code put},
 * and {@code remove} operations.
  * Assuming EM applications always need to instantiate this class, HTrace initialization is done in the constructor.
 */
@EvaluatorSide
@Private
public final class MemoryStoreImpl implements RemoteAccessibleMemoryStore {
  private static final Logger LOG = Logger.getLogger(MemoryStoreImpl.class.getName());

  private static final int QUEUE_THREAD_NUM = 8;
  private static final int QUEUE_SIZE = 1024;
  private static final int QUEUE_TIMEOUT_MS = 3000;

  /**
   * This map uses data types, represented as strings, for keys and inner {@code TreeMaps} for values.
   * Each inner {@code TreeMap} serves as a collection of data of the same data type.
   * {@code TreeMap}s are used for guaranteeing log(n) read and write operations, especially
   * {@code getRange()} and {@code removeRange()} which are ranged queries based on the ids.
   */
  private final Map<String, Map<Integer, Partition>> typeToPartitions = new HashMap<>();

  private final OperationRouter router;
  private final OperationResultAggregator resultAggregator;
  private final InjectionFuture<ElasticMemoryMsgSender> msgSender;

  private final Serializer serializer;

  /**
   * A counter for issuing ids for operations requested from local clients.
   */
  private final AtomicLong operationIdCounter = new AtomicLong(0);

  /**
   * A queue for operations requested from remote clients.
   * It maintains operations as sub operations corresponding to the routing result.
   * It also maintains corresponding partitions to avoid revisiting the routing table.
   */
  private final BlockingQueue<Tuple3<DataOperation, List<LongRange>, Partition>> subOperationQueue
      = new ArrayBlockingQueue<>(QUEUE_SIZE);
  private ExecutorService executorService = Executors.newFixedThreadPool(QUEUE_THREAD_NUM);


  @Inject
  private MemoryStoreImpl(final HTrace hTrace,
                          final OperationRouter router,
                          final OperationResultAggregator resultAggregator,
                          final InjectionFuture<ElasticMemoryMsgSender> msgSender,
                          final Serializer serializer) {
    hTrace.initialize();
    this.router = router;
    this.resultAggregator = resultAggregator;
    this.msgSender = msgSender;
    this.serializer = serializer;
    initThreads();
  }

  /**
   * Initialize threads that dequeue and execute operation from the {@code subOperationQueue}.
   * That is, these threads serve operations requested from remote clients.
   */
  private void initThreads() {
    for (int i = 0; i < QUEUE_THREAD_NUM; i++) {
      executorService.submit(new OperationThread());
    }
  }

  /**
   * Initialize partitions for a specific {@code dataType}.
   * Each partition holds sub data map, which composes whole data map for MemoryStore.
   */
  private synchronized void initPartitions(final String dataType) {
    if (typeToPartitions.containsKey(dataType)) {
      return;
    }

    final Map<Integer, Partition> initialPartitions = new HashMap<>();

    final List<Integer> localPartitions = router.getPartitions();
    for (final int partitionIdx : localPartitions) {
      initialPartitions.put(partitionIdx, new Partition());
    }

    // must put initialPartitions into typeToPartitions after completely initialize it
    typeToPartitions.put(dataType, initialPartitions);
  }

  /**
   * A runnable that dequeues and executes operations requested from remote clients.
   * Several threads are initiated at the beginning and run as long-running background services.
   */
  private final class OperationThread implements Runnable {
    // The max number of operations to drain per iteration
    private static final int DRAIN_SIZE = QUEUE_SIZE / QUEUE_THREAD_NUM;

    // Thread does not need to perform routing, because the queue element already has List<LongRange> and Partition.
    private final List<Tuple3<DataOperation, List<LongRange>, Partition>> drainedSubOperations =
        new ArrayList<>(DRAIN_SIZE);

    @Override
    public void run() {
      while (true) {
        // First, poll and execute a single operation.
        // Poll with a timeout will prevent busy waiting, when the queue is empty.
        try {
          final Tuple3<DataOperation, List<LongRange>, Partition> subOperation =
              subOperationQueue.poll(QUEUE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
          if (subOperation == null) {
            continue;
          }
          final DataOperation operation = subOperation.getFirst();
          final List<LongRange> subKeyRanges = subOperation.getSecond();
          final Partition partition = subOperation.getThird();

          final Map<Long, Object> result = partition.executeSubOperation(operation, subKeyRanges);
          resultAggregator.submitLocalResult(operation, result, Collections.EMPTY_LIST);
        } catch (final InterruptedException e) {
          LOG.log(Level.SEVERE, "Poll failed with InterruptedException", e);
          continue;
        }

        // Then, drain up to DRAIN_SIZE of the remaining queue and execute.
        // drainTo method is much faster than multiple polls.
        if (subOperationQueue.drainTo(drainedSubOperations, DRAIN_SIZE) == 0) {
          continue;
        }

        for (final Tuple3<DataOperation, List<LongRange>, Partition> subOperation : drainedSubOperations) {
          final DataOperation operation = subOperation.getFirst();
          final List<LongRange> subKeyRanges = subOperation.getSecond();
          final Partition partition = subOperation.getThird();

          final Map<Long, Object> result = partition.executeSubOperation(operation, subKeyRanges);
          resultAggregator.submitLocalResult(operation, result, Collections.EMPTY_LIST);
        }

        drainedSubOperations.clear();
      }
    }
  }

  /**
   * Partition class that has a {@code subDataMap}, which is an unit of EM's move.
   * Also it's a concurrency unit for data operations because it has a {@code ReadWriteLock},
   * which regulates accesses to {@code subDataMap}.
   */
  private final class Partition {
    /**
     * The map serves as a collection of data of the same data type.
     * It's implementation {@code TreeMap} is used for guaranteeing log(n) read and write operations, especially
     * {@code getRange()} and {@code removeRange()} which are ranged queries based on the ids.
     */
    private final NavigableMap<Long, Object> subDataMap = new TreeMap<>();

    /**
     * A read-write lock for {@code subDataMap} of the partition.
     * Let's set fairness option as true to prevent starvation.
     */
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock(true);

    /**
     * Executes sub operation on data keys assigned to this partition.
     * All operations both from remote and local clients are executed via this method.
     */
    public <T> Map<Long, T> executeSubOperation(final DataOperation<T> operation, final List<LongRange> subKeyRanges) {
      final DataOpType operationType = operation.getOperationType();

      final Map<Long, T> outputData = new HashMap<>();
      switch (operationType) {
      case PUT:
        rwLock.writeLock().lock();
        try {
          final NavigableMap<Long, T> dataKeyValueMap = operation.getDataKeyValueMap().get();

          for (final LongRange keyRange : subKeyRanges) {
            // extract matching entries from the input kv data map and put it all to subDataMap
            final NavigableMap<Long, T> subMap =
                dataKeyValueMap.subMap(keyRange.getMinimumLong(), true, keyRange.getMaximumLong(), true);
            subDataMap.putAll(subMap);
          }

          // PUT operations always succeed for all the ranges, because it overwrites map with the given input value.
          // So, outputData for PUT operations should be determined in other places.
        } finally {
          rwLock.writeLock().unlock();
        }
        break;
      case GET:
        rwLock.readLock().lock();
        try {
          for (final LongRange keyRange : subKeyRanges) {
            final Map<Long, T> partialOutput =
                (Map<Long, T>) subDataMap.subMap(keyRange.getMinimumLong(), true, keyRange.getMaximumLong(), true);
            outputData.putAll(partialOutput);
          }
        } finally {
          rwLock.readLock().unlock();
        }
        break;
      case REMOVE:
        rwLock.writeLock().lock();
        try {
          for (final LongRange keyRange : subKeyRanges) {
            final Map<Long, T> partialOutput =
                (Map<Long, T>) subDataMap.subMap(keyRange.getMinimumLong(), true, keyRange.getMaximumLong(), true);
            outputData.putAll(partialOutput);
          }
          subDataMap.keySet().removeAll(outputData.keySet());
        } finally {
          rwLock.writeLock().unlock();
        }
        break;
      default:
        throw new RuntimeException("Undefined operation");
      }

      return outputData;
    }

    public <T> Map<Long, T> getAll() {
      rwLock.readLock().lock();
      try {
        return (Map<Long, T>) ((TreeMap) subDataMap).clone();
      } finally {
        rwLock.readLock().unlock();
      }
    }

    public <T> Map<Long, T> removeAll() {
      final Map<Long, T> result;
      rwLock.writeLock().lock();
      try {
        result = (Map<Long, T>) ((TreeMap) subDataMap).clone();
        subDataMap.clear();
      } finally {
        rwLock.writeLock().unlock();
      }

      return result;
    }

    public int getNumUnits() {
      return subDataMap.size();
    }
  }

  /**
   * Executes an operation requested from a remote client.
   * It enqueues sub operations into corresponding Partitions' queues.
   */
  @Override
  public void onNext(final DataOperation operation) {

    final String dataType = operation.getDataType();

    final List<LongRange> dataKeyRanges = operation.getDataKeyRanges();

    // route key ranges of the operation
    final Pair<Map<Integer, List<LongRange>>, Map<String, List<LongRange>>> routingResult = router.route(dataKeyRanges);
    final Map<Integer, List<LongRange>> localKeyRangesMap = routingResult.getFirst();
    final Map<String, List<LongRange>> remoteKeyRangesMap = routingResult.getSecond();

    final int numSubOps = localKeyRangesMap.size() + 1; // +1 for remote ranges
    resultAggregator.registerOp(operation, numSubOps);

    if (!localKeyRangesMap.isEmpty()) {
      if (typeToPartitions.containsKey(dataType)) {
        enqueueOperation(operation, localKeyRangesMap);
      } else {
        if (operation.getOperationType() == DataOpType.PUT) {
          initPartitions(dataType);
          enqueueOperation(operation, localKeyRangesMap);
        }
      }
    }

    // treat remote ranges as failed ranges
    final List<LongRange> failedRanges = new LinkedList<>();
    for (final List<LongRange> remoteRanges : remoteKeyRangesMap.values()) {
      failedRanges.addAll(remoteRanges);
    }
    // submit it as a local result, because we do not even start the remote operation
    resultAggregator.submitLocalResult(operation, Collections.EMPTY_MAP, failedRanges);
  }

  /**
   * Enqueues local sub operations to {@code subOperationQueue}.
   */
  private void enqueueOperation(final DataOperation operation,
                                final Map<Integer, List<LongRange>> partitionedKeyRangesMap) {
    final Map<Integer, Partition> partitions = typeToPartitions.get(operation.getDataType());

    for (final Map.Entry<Integer, List<LongRange>> entry : partitionedKeyRangesMap.entrySet()) {
      final int partitionId = entry.getKey();
      final List<LongRange> subKeyRanges = entry.getValue();

      final Partition partition = partitions.get(partitionId);
      try {
        subOperationQueue.put(new Tuple3<>(operation, subKeyRanges, partition));
      } catch (final InterruptedException e) {
        LOG.log(Level.SEVERE, "Enqueue failed with InterruptedException", e);
      }
    }
  }

  /**
   * Executes an operation requested from a local client.
   */
  private <T> void executeOperation(final DataOperation<T> operation) {

    final List<LongRange> dataKeyRanges = operation.getDataKeyRanges();

    // route key ranges of the operation
    final Pair<Map<Integer, List<LongRange>>, Map<String, List<LongRange>>> routingResult = router.route(dataKeyRanges);
    final Map<Integer, List<LongRange>> localKeyRangesMap = routingResult.getFirst();
    final Map<String, List<LongRange>> remoteKeyRangesMap = routingResult.getSecond();

    final int numSubOps = remoteKeyRangesMap.size() + 1; // +1 for local operation
    resultAggregator.registerOp(operation, numSubOps);

    // send remote operations first and execute local operations
    sendOperationToRemoteStores(operation, remoteKeyRangesMap);
    final Map<Long, T> localOutputData = executeLocalOperation(operation, localKeyRangesMap);

    // submit the local result and wait until all remote operations complete
    resultAggregator.submitLocalResult(operation, localOutputData, Collections.EMPTY_LIST);
  }

  /**
   * Executes sub local operations directly, not via queueing into Partitions.
   */
  private <T> Map<Long, T> executeLocalOperation(final DataOperation<T> operation,
                                                 final Map<Integer, List<LongRange>> partitionedKeyRanges) {
    if (partitionedKeyRanges.isEmpty()) {
      return Collections.EMPTY_MAP;
    }

    final String dataType = operation.getDataType();
    if (!typeToPartitions.containsKey(dataType)) {
      if (operation.getOperationType() == DataOpType.PUT) {
        initPartitions(dataType);
      } else {
        return Collections.EMPTY_MAP;
      }
    }

    final Map<Integer, Partition> partitions = typeToPartitions.get(operation.getDataType());

    final Map<Long, T> outputData;
    final Iterator<Map.Entry<Integer, List<LongRange>>> rangeIterator = partitionedKeyRanges.entrySet().iterator();

    // first execute a head range to reuse the returned map object for a return map
    if (rangeIterator.hasNext()) {
      final Map.Entry<Integer, List<LongRange>> entry = rangeIterator.next();
      final Partition partition = partitions.get(entry.getKey());
      final List<LongRange> subKeyRanges = entry.getValue();

      outputData = partition.executeSubOperation(operation, subKeyRanges);
    } else {
      return Collections.EMPTY_MAP;
    }

    // execute remaining ranges if exist
    while (rangeIterator.hasNext()) {
      final Map.Entry<Integer, List<LongRange>> entry = rangeIterator.next();
      final Partition partition = partitions.get(entry.getKey());
      final List<LongRange> subKeyRanges = entry.getValue();

      final Map<Long, T> partialOutput = partition.executeSubOperation(operation, subKeyRanges);
      outputData.putAll(partialOutput);
    }

    return outputData;
  }

  /**
   * Sends sub operations to target remote evaluators.
   */
  private <T> void sendOperationToRemoteStores(final DataOperation<T> operation,
                                               final Map<String, List<LongRange>> remoteKeyRangesMap) {

    if (remoteKeyRangesMap.isEmpty()) {
      return;
    }

    final Codec codec = serializer.getCodec(operation.getDataType());

    // send sub operations to all remote stores that owns partial range of the main operation (RemoteOpMsg)
    for (final Map.Entry<String, List<LongRange>> remoteEntry : remoteKeyRangesMap.entrySet()) {
      try (final TraceScope traceScope = Trace.startSpan("SEND_REMOTE_OP")) {
        final TraceInfo traceInfo = TraceInfo.fromSpan(traceScope.getSpan());

        final String targetEvalId = remoteEntry.getKey();
        final List<LongRange> keyRanges = remoteEntry.getValue();

        final List<UnitIdPair> dataKVPairList;
        if (operation.getOperationType() == DataOpType.PUT) {
          final NavigableMap<Long, T> keyValueMap = operation.getDataKeyValueMap().get();

          dataKVPairList = new LinkedList<>();

          // encode all data value and put them into dataKVPairList
          for (final LongRange range : keyRanges) {
            // extract range-matching entries from the input data map
            final Map<Long, T> subMap = keyValueMap.subMap(range.getMinimumLong(), true, range.getMaximumLong(), true);

            for (final Map.Entry<Long, T> dataKVPair : subMap.entrySet()) {
              final ByteBuffer encodedData = ByteBuffer.wrap(codec.encode(dataKVPair.getValue()));
              dataKVPairList.add(new UnitIdPair(encodedData, dataKVPair.getKey()));
            }
          }
        } else {
          // For GET and REMOVE operations, set dataKVPairList as an empty list
          dataKVPairList = Collections.EMPTY_LIST;
        }

        msgSender.get().sendRemoteOpMsg(operation.getOrigEvalId().get(), targetEvalId,
            operation.getOperationType(), operation.getDataType(), keyRanges, dataKVPairList,
            operation.getOperationId(), traceInfo);
      }
    }
  }

  @Override
  public <T> Pair<Long, Boolean> put(final String dataType, final long id, final T value) {
    if (value == null) {
      return new Pair<>(id, false);
    }

    final String operationId = Long.toString(operationIdCounter.getAndIncrement());

    final DataOperation<T> operation = new DataOperation<>(Optional.<String>empty(), operationId, DataOpType.PUT,
        dataType, id, Optional.of(value));

    executeOperation(operation);

    return new Pair<>(id, operation.getFailedRanges().isEmpty());
  }

  @Override
  public <T> Map<Long, Boolean> putList(final String dataType, final List<Long> ids, final List<T> values) {
    if (ids.size() != values.size()) {
      throw new RuntimeException("Different list sizes: ids " + ids.size() + ", values " + values.size());
    }

    final String operationId = Long.toString(operationIdCounter.getAndIncrement());

    final List<LongRange> longRangeSet = new ArrayList<>(LongRangeUtils.generateDenseLongRanges(new TreeSet<>(ids)));

    final NavigableMap<Long, T> dataKeyValueMap = new TreeMap<>();
    for (int idx = 0; idx < ids.size(); idx++) {
      dataKeyValueMap.put(ids.get(idx), values.get(idx));
    }

    final DataOperation<T> operation = new DataOperation<>(Optional.<String>empty(), operationId, DataOpType.PUT,
        dataType, longRangeSet, Optional.of(dataKeyValueMap));

    executeOperation(operation);

    return getResultForPutList(ids, operation.getFailedRanges());
  }

  /**
   * Returns a result map for putList operation.
   * The map has entries for all input data keys and corresponding boolean values
   * that are false for failed keys and true for succeeded keys
   */
  private Map<Long, Boolean> getResultForPutList(final List<Long> inputKeys, final List<LongRange> failedKeyRanges) {
    final Map<Long, Boolean> resultMap = new HashMap<>(inputKeys.size());
    if (failedKeyRanges.isEmpty()) {
      for (final long key : inputKeys) {
        resultMap.put(key, true);
      }
      return resultMap;
    }

    // sort failedRanges and keys to compare them
    Collections.sort(failedKeyRanges, LongRangeUtils.LONG_RANGE_COMPARATOR);
    Collections.sort(inputKeys);

    // set the result of input keys: set false for elements included in failedRanges and true for others
    final Iterator<LongRange> rangeIterator = failedKeyRanges.iterator();
    LongRange range = rangeIterator.next();
    int keyIdx;
    for (keyIdx = 0; keyIdx < inputKeys.size(); keyIdx++) {
      final long key = inputKeys.get(keyIdx);
      if (range.getMinimumLong() > key) {
        resultMap.put(key, false);
        // go to next key
        continue;
      }

      if (range.getMaximumLong() < key) {
        if (rangeIterator.hasNext()) {
          // go to next range
          range = rangeIterator.next();
          keyIdx--;
          continue;
        } else {
          // break from the loop
          // then a below loop will put all remaining keys to resultMap
          break;
        }
      }
      resultMap.put(key, true);
    }

    // put all remaining keys to resultMap
    for (; keyIdx < inputKeys.size(); keyIdx++) {
      resultMap.put(inputKeys.get(keyIdx), false);
    }
    return resultMap;
  }

  @Override
  public <T> Pair<Long, T> get(final String dataType, final long id) {

    final String operationId = Long.toString(operationIdCounter.getAndIncrement());

    final DataOperation<T> operation = new DataOperation<>(Optional.<String>empty(), operationId, DataOpType.GET,
        dataType, id, Optional.<T>empty());

    executeOperation(operation);

    final T outputData = operation.getOutputData().get(id);

    return outputData == null ? null : new Pair<>(id, outputData);
  }

  @Override
  public <T> Map<Long, T> getAll(final String dataType) {
    if (!typeToPartitions.containsKey(dataType)) {
      return Collections.EMPTY_MAP;
    }

    final Map<Long, T> result;
    final Collection<Partition> partitions = typeToPartitions.get(dataType).values();

    final Iterator<Partition> partitionIterator = partitions.iterator();

    // first execute on a head partition to reuse the returned map object for a return map
    if (partitionIterator.hasNext()) {
      final Partition partition = partitionIterator.next();
      result = partition.getAll();
    } else {
      return Collections.EMPTY_MAP;
    }

    // execute on remaining partitions if exist
    while (partitionIterator.hasNext()) {
      final Partition partition = partitionIterator.next();
      // huge memory pressure may happen here
      result.putAll((Map<Long, T>) partition.getAll());
    }

    return result;
  }

  @Override
  public <T> Map<Long, T> getRange(final String dataType, final long startId, final long endId) {

    final String operationId = Long.toString(operationIdCounter.getAndIncrement());

    final DataOperation<T> operation = new DataOperation<>(Optional.<String>empty(), operationId, DataOpType.GET,
        dataType, new LongRange(startId, endId), Optional.<NavigableMap<Long, T>>empty());

    executeOperation(operation);

    return operation.getOutputData();
  }

  @Override
  public <T> Pair<Long, T> remove(final String dataType, final long id) {

    final String operationId = Long.toString(operationIdCounter.getAndIncrement());
    final DataOperation<T> operation = new DataOperation<>(Optional.<String>empty(), operationId, DataOpType.REMOVE,
        dataType, id, Optional.<T>empty());

    executeOperation(operation);

    final T outputData = operation.getOutputData().get(id);

    return outputData == null ? null : new Pair<>(id, outputData);
  }

  @Override
  public <T> Map<Long, T> removeAll(final String dataType) {
    if (!typeToPartitions.containsKey(dataType)) {
      return Collections.EMPTY_MAP;
    }

    final Map<Long, T> result;
    final Collection<Partition> partitions = typeToPartitions.get(dataType).values();


    final Iterator<Partition> partitionIterator = partitions.iterator();

    // first execute on a head partition to reuse the returned map object for a return map
    if (partitionIterator.hasNext()) {
      final Partition partition = partitionIterator.next();
      result = partition.removeAll();
    } else {
      return Collections.EMPTY_MAP;
    }

    // execute on remaining partitions if exist
    while (partitionIterator.hasNext()) {
      final Partition partition = partitionIterator.next();
      // huge memory pressure may happen here
      result.putAll((Map<Long, T>) partition.removeAll());
    }

    return result;
  }

  @Override
  public <T> Map<Long, T> removeRange(final String dataType, final long startId, final long endId) {

    final String operationId = Long.toString(operationIdCounter.getAndIncrement());

    final DataOperation<T> operation = new DataOperation<>(Optional.<String>empty(), operationId, DataOpType.REMOVE,
        dataType, new LongRange(startId, endId), Optional.<NavigableMap<Long, T>>empty());

    executeOperation(operation);

    return operation.getOutputData();
  }

  @Override
  public Set<String> getDataTypes() {
    return new HashSet<>(typeToPartitions.keySet());
  }

  @Override
  public int getNumUnits(final String dataType) {
    if (typeToPartitions.containsKey(dataType)) {
      return 0;
    }

    int numUnits = 0;
    final Collection<Partition> partitions = typeToPartitions.get(dataType).values();
    for (final Partition partition : partitions) {
      numUnits += partition.getNumUnits();
    }
    return numUnits;
  }
}
