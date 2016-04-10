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

import edu.snu.cay.services.em.avro.AvroLongRange;
import edu.snu.cay.services.em.avro.DataOpType;
import edu.snu.cay.services.em.avro.UnitIdPair;
import edu.snu.cay.services.em.evaluator.api.RemoteAccessibleMemoryStore;
import edu.snu.cay.services.em.msg.api.ElasticMemoryMsgSender;
import edu.snu.cay.services.em.serialize.Serializer;
import edu.snu.cay.utils.LongRangeUtils;
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
public final class MemoryStoreImpl implements RemoteAccessibleMemoryStore<Long> {
  private static final Logger LOG = Logger.getLogger(MemoryStoreImpl.class.getName());

  private static final int QUEUE_SIZE = 100;
  private static final int QUEUE_TIMEOUT_MS = 3000;


  /**
   * This map uses data types, represented as strings, for keys and inner {@code TreeMaps} for values.
   * Each inner {@code TreeMap} serves as a collection of data of the same data type.
   * {@code TreeMap}s are used for guaranteeing log(n) read and write operations, especially
   * {@code getRange()} and {@code removeRange()} which are ranged queries based on the ids.
   */
  private final Map<String, TreeMap<Long, Object>> dataMap;

  /**
   * Used for synchronization between operations.
   * {@code get} uses the read lock, while {@code put} and {@code remove} use the write lock.
   */
  private final ReadWriteLock readWriteLock;

  private final OperationRouter<Long> router;
  private final OperationResultAggregator resultAggregator;
  private final InjectionFuture<ElasticMemoryMsgSender> msgSender;

  private final Serializer serializer;

  /**
   * A counter for issuing ids for operations requested from local clients.
   */
  private final AtomicLong operationIdCounter = new AtomicLong(0);

  /**
   * A queue for enqueueing operations from remote memory stores.
   */
  private final BlockingQueue<DataOperation> operationQueue = new ArrayBlockingQueue<>(QUEUE_SIZE);
  private final ExecutorService executorService = Executors.newSingleThreadExecutor();

  @Inject
  private MemoryStoreImpl(final HTrace hTrace,
                          final OperationRouter<Long> router,
                          final OperationResultAggregator resultAggregator,
                          final InjectionFuture<ElasticMemoryMsgSender> msgSender,
                          final Serializer serializer) {
    hTrace.initialize();
    this.router = router;
    this.resultAggregator = resultAggregator;
    this.msgSender = msgSender;
    this.serializer = serializer;
    dataMap = new HashMap<>();
    readWriteLock = new ReentrantReadWriteLock(true);
    initialize();
  }

  private void initialize() {
    executorService.execute(new OperationThread());
  }

  @Override
  public void onNext(final DataOperation dataOperation) {
    try {
      operationQueue.put(dataOperation);
    } catch (final InterruptedException e) {
      LOG.log(Level.SEVERE, "Interrupted while waiting for enqueueing an operation", e);
    }
  }

  private final class OperationThread implements Runnable {

    private final int drainSize = QUEUE_SIZE / 10; // The max number of operations to drain per iteration
    private final List<DataOperation> drainedOperations = new ArrayList<>(drainSize);

    /**
     * A loop that dequeues operations and executes them.
     * Dequeues are only performed through this thread.
     */
    @Override
    public void run() {

      while (true) {
        // First, poll and execute a single operation.
        // Poll with a timeout will prevent busy waiting, when the queue is empty.
        try {
          final DataOperation operation = operationQueue.poll(QUEUE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
          if (operation == null) {
            continue;
          }
          executeOperation(operation);
        } catch (final InterruptedException e) {
          LOG.log(Level.SEVERE, "Poll failed with InterruptedException", e);
          continue;
        }

        // Then, drain up to drainSize of the remaining queue and execute.
        // drainTo method is much faster than multiple polls.
        if (operationQueue.drainTo(drainedOperations, drainSize) == 0) {
          continue;
        }

        for (final DataOperation operation : drainedOperations) {
          executeOperation(operation);
        }

        drainedOperations.clear();
      }
    }
  }


  private <T> void executeOperation(final DataOperation<T> operation) {

    final List<LongRange> dataKeyRanges = operation.getDataKeyRanges();

    final Pair<List<LongRange>, Map<String, List<LongRange>>> routingResult = router.route(dataKeyRanges);

    final List<LongRange> localKeyRanges = routingResult.getFirst();
    final Map<String, List<LongRange>> remoteKeyRangesMap = routingResult.getSecond();

    if (operation.isFromLocalClient()) {
      final int numSubOps = remoteKeyRangesMap.size() + 1; // +1 for local operation
      resultAggregator.registerOperation(operation, numSubOps);
    }

    // send remote operation only when the operation is requested from the local client
    // That is, it does not support rerouting.
    if (operation.isFromLocalClient() && !remoteKeyRangesMap.isEmpty()) {
      sendOperationsToRemoteStores(operation, remoteKeyRangesMap);
    }

    // execute local operations, after sending remote operations above
    final Map<Long, T> localOutputData;
    if (!localKeyRanges.isEmpty()) {
      localOutputData = executeLocalOperation(operation, localKeyRanges);
    } else {
      localOutputData = Collections.EMPTY_MAP;
    }

    // handle local result
    if (operation.isFromLocalClient()) {
      // a. submit the local result and wait until all remote operations complete
      resultAggregator.submitLocalResult(operation, localOutputData);
    } else {
      // b. send the local result to the original store
      final Collection<List<LongRange>> failedRanges = remoteKeyRangesMap.values();
      sendResultToOrigin(operation, localOutputData, failedRanges);
    }
  }

  private <T> Map<Long, T> executeLocalOperation(final DataOperation<T> operation, final List<LongRange> subKeyRanges) {
    final DataOpType operationType = operation.getOperationType();
    final String dataType = operation.getDataType();

    final Map<Long, T> outputData = new HashMap<>();
    switch (operationType) {
    case PUT:
      readWriteLock.writeLock().lock();
      try {
        if (!dataMap.containsKey(dataType)) {
          dataMap.put(dataType, new TreeMap<Long, Object>());
        }

        final SortedMap<Long, T> dataKeyValueMap = operation.getDataKeyValueMap().get();
        final NavigableMap<Long, T> innerMap = (NavigableMap<Long, T>) dataMap.get(dataType);

        for (final LongRange keyRange : subKeyRanges) {
          // extract range-matching entries from the map and put it all to dataMap
          final SortedMap<Long, T> subMap;
          if (keyRange.getMaximumLong() == Long.MAX_VALUE) {
            subMap = dataKeyValueMap.tailMap(keyRange.getMinimumLong());
          } else {
            // +1 to include maximum value
            subMap = dataKeyValueMap.subMap(keyRange.getMinimumLong(), keyRange.getMaximumLong() + 1);
          }

          innerMap.putAll(subMap);
        }
      } finally {
        readWriteLock.writeLock().unlock();
      }
      break;
    case GET:
      readWriteLock.readLock().lock();
      try {
        if (!dataMap.containsKey(dataType)) {
          break;
        }

        final NavigableMap<Long, T> innerMap = (NavigableMap<Long, T>) dataMap.get(dataType);

        for (final LongRange keyRange : subKeyRanges) {
          final Map<Long, T> partialOutput =
              innerMap.subMap(keyRange.getMinimumLong(), true, keyRange.getMaximumLong(), true);
          outputData.putAll(partialOutput);
        }
      } finally {
        readWriteLock.readLock().unlock();
      }
      break;
    case REMOVE:
      readWriteLock.writeLock().lock();
      try {
        if (!dataMap.containsKey(dataType)) {
          break;
        }

        final NavigableMap<Long, T> innerMap = (NavigableMap<Long, T>) dataMap.get(dataType);

        for (final LongRange keyRange : subKeyRanges) {
          final Map<Long, T> partialOutput =
              innerMap.subMap(keyRange.getMinimumLong(), true, keyRange.getMaximumLong(), true);
          outputData.putAll(partialOutput);
        }
        innerMap.keySet().removeAll(outputData.keySet());
      } finally {
        readWriteLock.writeLock().unlock();
      }
      break;
    default:
      throw new RuntimeException("Undefined operation");
    }

    return outputData;
  }

  /**
   * Sends sub operations to target remote evaluators.
   */
  private <T> void sendOperationsToRemoteStores(final DataOperation<T> operation,
                                                final Map<String, List<LongRange>> remoteKeyRangesMap) {

    final Codec codec = serializer.getCodec(operation.getDataType());

    // send sub operations to all remote stores that owns partial range of the main operation (RemoteOpMsg)
    for (final Map.Entry<String, List<LongRange>> remoteEntry : remoteKeyRangesMap.entrySet()) {
      try (final TraceScope traceScope = Trace.startSpan("SEND_REMOTE_OP")) {
        final TraceInfo traceInfo = TraceInfo.fromSpan(traceScope.getSpan());

        final String targetEvalId = remoteEntry.getKey();
        final List<LongRange> keyRanges = remoteEntry.getValue();

        final List<UnitIdPair> dataKVPairList;
        if (operation.getOperationType() == DataOpType.PUT) {
          final SortedMap<Long, T> keyValueMap = operation.getDataKeyValueMap().get();

          dataKVPairList = new LinkedList<>();

          // encode all data value and put them into dataKVPairList
          for (final LongRange range : keyRanges) {
            // extract range-matching entries from the map and put it all to dataMap
            final Map<Long, T> subMap;
            if (range.getMaximumLong() == Long.MAX_VALUE) {
              subMap = keyValueMap.tailMap(range.getMinimumLong());
            } else {
              // +1 to include maximum value
              subMap = keyValueMap.subMap(range.getMinimumLong(), range.getMaximumLong() + 1);
            }

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

  /**
   * Sends the result to the original store.
   */
  private <T> void sendResultToOrigin(final DataOperation<T> operation, final Map<Long, T> localOutputData,
                                      final Collection<List<LongRange>> remoteKeyRanges) {
    // send the original store the result (RemoteOpResultMsg)
    try (final TraceScope traceScope = Trace.startSpan("SEND_REMOTE_RESULT")) {
      final String dataType = operation.getDataType();
      final Codec codec = serializer.getCodec(dataType);

      final Optional<String> origEvalId = operation.getOrigEvalId();

      final List<UnitIdPair> dataKVPairList;
      if (operation.getOperationType() == DataOpType.GET || operation.getOperationType() == DataOpType.REMOVE) {
        dataKVPairList = new LinkedList<>();

        for (final Map.Entry<Long, T> dataKVPair : localOutputData.entrySet()) {
          final ByteBuffer encodedData = ByteBuffer.wrap(codec.encode(dataKVPair.getValue()));
          dataKVPairList.add(new UnitIdPair(encodedData, dataKVPair.getKey()));
        }
      } else {
        dataKVPairList = Collections.EMPTY_LIST;
      }

      final List<LongRange> failedRanges = new ArrayList<>(remoteKeyRanges.size());
      for (final List<LongRange> rangeList : remoteKeyRanges) {
        failedRanges.addAll(rangeList);
      }

      msgSender.get().sendRemoteOpResultMsg(origEvalId.get(), dataKVPairList, failedRanges, operation.getOperationId(),
          TraceInfo.fromSpan(traceScope.getSpan()));
    }
  }

  @Override
  public <T> Pair<Long, Boolean> put(final String dataType, final Long id, final T value) {
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

    final SortedMap<Long, T> dataKeyValueMap = new TreeMap<>();
    for (int idx = 0; idx < ids.size(); idx++) {
      dataKeyValueMap.put(ids.get(idx), values.get(idx));
    }

    final DataOperation<T> operation = new DataOperation<>(Optional.<String>empty(), operationId, DataOpType.PUT,
        dataType, longRangeSet, Optional.of(dataKeyValueMap));

    executeOperation(operation);

    return setResultForPutList(ids, operation.getFailedRanges());
  }

  /**
   * Returns a result map for putList operation.
   * The map has entries for all input data keys and corresponding boolean values
   * that are false for failed keys and true for succeeded keys
   */
  private Map<Long, Boolean> setResultForPutList(final List<Long> keys, final List<AvroLongRange> failedRanges) {
    final Map<Long, Boolean> resultMap = new HashMap<>(keys.size());
    if (failedRanges.isEmpty()) {
      for (final long key : keys) {
        resultMap.put(key, true);
      }
      return resultMap;
    }

    // sort failedRanges and ids to compare them
    final NavigableSet<LongRange> failedRangeSet = LongRangeUtils.createLongRangeSet();
    for (final AvroLongRange range : failedRanges) {
      failedRangeSet.add(new LongRange(range.getMin(), range.getMax()));
    }
    Collections.sort(keys);

    // set the result of input keys
    // set false for elements included in failedRanges and true for others
    final Iterator<LongRange> rangeIterator = failedRangeSet.iterator();
    LongRange range = rangeIterator.next();
    int keyIdx;
    for (keyIdx = 0; keyIdx < keys.size(); keyIdx++) {
      final long key = keys.get(keyIdx);
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
    for (; keyIdx < keys.size(); keyIdx++) {
      resultMap.put(keys.get(keyIdx), false);
    }
    return resultMap;
  }

  @Override
  public <T> Pair<Long, T> get(final String dataType, final Long id) {

    final String operationId = Long.toString(operationIdCounter.getAndIncrement());

    final DataOperation<T> operation = new DataOperation<>(Optional.<String>empty(), operationId, DataOpType.GET,
        dataType, id, Optional.<T>empty());

    executeOperation(operation);

    final T outputData = operation.getOutputData().get(id);

    return outputData == null ? null : new Pair<>(id, outputData);
  }

  @Override
  public <T> Map<Long, T> getAll(final String dataType) {
    readWriteLock.readLock().lock();

    try {
      if (!dataMap.containsKey(dataType)) {
        return new TreeMap<>();
      }
      return (Map<Long, T>)dataMap.get(dataType).clone();

    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  @Override
  public <T> Map<Long, T> getRange(final String dataType, final Long startId, final Long endId) {

    final String operationId = Long.toString(operationIdCounter.getAndIncrement());

    final DataOperation<T> operation = new DataOperation<>(Optional.<String>empty(), operationId, DataOpType.GET,
        dataType, new LongRange(startId, endId), Optional.<SortedMap<Long, T>>empty());

    executeOperation(operation);

    return operation.getOutputData();
  }

  @Override
  public <T> Pair<Long, T> remove(final String dataType, final Long id) {

    final String operationId = Long.toString(operationIdCounter.getAndIncrement());
    final DataOperation<T> operation = new DataOperation<>(Optional.<String>empty(), operationId, DataOpType.REMOVE,
        dataType, id, Optional.<T>empty());

    executeOperation(operation);

    final T outputData = operation.getOutputData().get(id);

    return outputData == null ? null : new Pair<>(id, outputData);
  }

  @Override
  public <T> Map<Long, T> removeAll(final String dataType) {
    readWriteLock.writeLock().lock();

    try {
      if (!dataMap.containsKey(dataType)) {
        return new TreeMap<>();
      }
      return (Map<Long, T>)dataMap.remove(dataType);

    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  @Override
  public <T> Map<Long, T> removeRange(final String dataType, final Long startId, final Long endId) {

    final String operationId = Long.toString(operationIdCounter.getAndIncrement());

    final DataOperation<T> operation = new DataOperation<>(Optional.<String>empty(), operationId, DataOpType.REMOVE,
        dataType, new LongRange(startId, endId), Optional.<SortedMap<Long, T>>empty());

    executeOperation(operation);

    return operation.getOutputData();
  }

  @Override
  public Set<String> getDataTypes() {
    readWriteLock.readLock().lock();

    try {
      final Set<String> dataTypeSet = new HashSet<>(dataMap.keySet().size());
      for (final Map.Entry<String, TreeMap<Long, Object>> entry : dataMap.entrySet()) {
        if (!entry.getValue().isEmpty()) {
          dataTypeSet.add(entry.getKey());
        }
      }

      return dataTypeSet;

    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  @Override
  public int getNumUnits(final String dataType) {
    readWriteLock.readLock().lock();

    try {
      if (!dataMap.containsKey(dataType)) {
        return 0;
      }
      return dataMap.get(dataType).size();

    } finally {
      readWriteLock.readLock().unlock();
    }
  }
}
