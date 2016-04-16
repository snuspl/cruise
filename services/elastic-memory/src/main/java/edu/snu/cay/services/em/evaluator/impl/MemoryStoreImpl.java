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
import edu.snu.cay.services.em.common.parameters.NumStoreThreads;
import edu.snu.cay.services.em.evaluator.api.DataOperation;
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
import org.apache.reef.tang.annotations.Parameter;
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

  private static final int QUEUE_SIZE = 1024;
  private static final int QUEUE_TIMEOUT_MS = 3000;

  /**
   * This map uses data types, represented as strings, for keys and inner {@code TreeMaps} for values.
   * Each inner {@code TreeMap} serves as a collection of data of the same data type.
   * {@code TreeMap}s are used for guaranteeing log(n) read and write operations, especially
   * {@code getRange()} and {@code removeRange()} which are ranged queries based on the ids.
   */
  private final Map<String, Map<Integer, Block>> typeToBlocks = new HashMap<>();

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
   * Its element is composed of a operation, a sub key range, and a corresponding block id.
   */
  private final BlockingQueue<Tuple3<LongKeyOperation, LongRange, Integer>> subOperationQueue
      = new ArrayBlockingQueue<>(QUEUE_SIZE);

  @Inject
  private MemoryStoreImpl(final HTrace hTrace,
                          final OperationRouter router,
                          final OperationResultAggregator resultAggregator,
                          final InjectionFuture<ElasticMemoryMsgSender> msgSender,
                          final Serializer serializer,
                          @Parameter(NumStoreThreads.class) final int numStoreThreads) {
    hTrace.initialize();
    this.router = router;
    this.resultAggregator = resultAggregator;
    this.msgSender = msgSender;
    this.serializer = serializer;
    initExecutor(numStoreThreads);
  }

  /**
   * Initialize threads that dequeue and execute operation from the {@code subOperationQueue}.
   * That is, these threads serve operations requested from remote clients.
   */
  private void initExecutor(final int numStoreThreads) {
    final ExecutorService executor = Executors.newFixedThreadPool(numStoreThreads);
    for (int i = 0; i < numStoreThreads; i++) {
      executor.submit(new OperationThread());
    }
  }

  /**
   * Initialize blocks for a specific {@code dataType}.
   * Each block holds the subset of the data that is assigned to this MemoryStore.
   */
  private synchronized void initBlocks(final String dataType) {
    if (typeToBlocks.containsKey(dataType)) {
      return;
    }

    final Map<Integer, Block> initialBlocks = new HashMap<>();
    for (final int blockIdx : router.getLocalBlockIds()) {
      initialBlocks.put(blockIdx, new Block());
    }

    // must put initialBlocks into typeToBlocks after completely initialize it
    typeToBlocks.put(dataType, initialBlocks);
  }

  /**
   * A runnable that dequeues and executes operations requested from remote clients.
   * Several threads are initiated at the beginning and run as long-running background services.
   */
  private final class OperationThread implements Runnable {

    @Override
    public void run() {
      while (true) {
        // First, poll and execute a single operation.
        // Poll with a timeout will prevent busy waiting, when the queue is empty.
        try {
          final Tuple3<LongKeyOperation, LongRange, Integer> subOperation =
              subOperationQueue.poll(QUEUE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
          if (subOperation == null) {
            continue;
          }
          handleSubOperation(subOperation);
        } catch (final InterruptedException e) {
          LOG.log(Level.SEVERE, "Poll failed with InterruptedException", e);
        }
      }
    }

    private void handleSubOperation(final Tuple3<LongKeyOperation, LongRange, Integer> subOperation) {
      final LongKeyOperation operation = subOperation.getFirst();
      final LongRange subKeyRange = subOperation.getSecond();
      final int blockId = subOperation.getThird();

      final Optional<String> remoteEvalId = router.resolveEval(blockId);
      final boolean isLocal = !remoteEvalId.isPresent();
      if (isLocal) {
        final Block block = typeToBlocks.get(operation.getDataType()).get(blockId);
        final Map<Long, Object> result = block.executeSubOperation(operation, subKeyRange);
        resultAggregator.submitLocalResult(operation, result, Collections.EMPTY_LIST);
      } else {
        // treat remote ranges as failed ranges, because we do not allow more than one hop in remote access
        final List<LongRange> failedRanges = new ArrayList<>(1);
        failedRanges.add(subKeyRange);

        // submit it as a local result, because we do not even start the remote operation
        resultAggregator.submitLocalResult(operation, Collections.EMPTY_MAP, failedRanges);
      }
    }
  }

  /**
   * Block class that has a {@code subDataMap}, which is an unit of EM's move.
   * Also it's a concurrency unit for data operations because it has a {@code ReadWriteLock},
   * which regulates accesses to {@code subDataMap}.
   */
  private final class Block {
    /**
     * The map serves as a collection of data of the same data type.
     * It's implementation {@code TreeMap} is used for guaranteeing log(n) read and write operations, especially
     * {@code getRange()} and {@code removeRange()} which are ranged queries based on the ids.
     */
    private final NavigableMap<Long, Object> subDataMap = new TreeMap<>();

    /**
     * A read-write lock for {@code subDataMap} of the block.
     * Let's set fairness option as true to prevent starvation.
     */
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock(true);

    /**
     * Executes sub operation on data keys assigned to this block.
     * All operations both from remote and local clients are executed via this method.
     */
    private <V> Map<Long, V> executeSubOperation(final LongKeyOperation<V> operation, final LongRange keyRange) {
      final DataOpType operationType = operation.getOperationType();

      final Map<Long, V> outputData = new HashMap<>();
      switch (operationType) {
      case PUT:
        rwLock.writeLock().lock();
        try {
          final NavigableMap<Long, V> dataKeyValueMap = operation.getDataKeyValueMap().get();

          // extract matching entries from the input kv data map and put it all to subDataMap
          final NavigableMap<Long, V> subMap =
              dataKeyValueMap.subMap(keyRange.getMinimumLong(), true, keyRange.getMaximumLong(), true);
          subDataMap.putAll(subMap);

          // PUT operations always succeed for all the ranges, because it overwrites map with the given input value.
          // So, outputData for PUT operations should be determined in other places.
        } finally {
          rwLock.writeLock().unlock();
        }
        break;
      case GET:
        rwLock.readLock().lock();
        try {
          outputData.putAll((Map<Long, V>) subDataMap.subMap(keyRange.getMinimumLong(), true,
              keyRange.getMaximumLong(), true));
        } finally {
          rwLock.readLock().unlock();
        }
        break;
      case REMOVE:
        rwLock.writeLock().lock();
        try {
          outputData.putAll((Map<Long, V>) subDataMap.subMap(keyRange.getMinimumLong(), true,
              keyRange.getMaximumLong(), true));
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

    /**
     * Returns all data in a block.
     * It is for supporting getAll method of MemoryStore.
     */
    private <V> Map<Long, V> getAll() {
      rwLock.readLock().lock();
      try {
        return (Map<Long, V>) ((TreeMap) subDataMap).clone();
      } finally {
        rwLock.readLock().unlock();
      }
    }

    /**
     * Removes all data in a block.
     * It is for supporting removeAll method of MemoryStore.
     */
    private <V> Map<Long, V> removeAll() {
      final Map<Long, V> result;
      rwLock.writeLock().lock();
      try {
        result = (Map<Long, V>) ((TreeMap) subDataMap).clone();
        subDataMap.clear();
      } finally {
        rwLock.writeLock().unlock();
      }

      return result;
    }

    /**
     * Returns a number of data in a block.
     * It is for supporting getNumUnits method of MemoryStore.
     */
    private int getNumUnits() {
      return subDataMap.size();
    }
  }

  /**
   * Enqueues operations requested from a remote client.
   * The enqueued operations are executed by {@code OperationThread}s.
   */
  @Override
  public void onNext(final DataOperation<Long> op) {
    final LongKeyOperation operation = (LongKeyOperation) op;
    final String dataType = operation.getDataType();

    // route key ranges of the operation
    final List<LongRange> dataKeyRanges = operation.getDataKeyRanges();

    final Map<Integer, LongRange> blockToSubKeyRangeMap;

    final Iterator<LongRange> rangeIterator = dataKeyRanges.iterator();

    // handle the first case separately to reuse a returned map object
    if (rangeIterator.hasNext()) {
      final LongRange keyRange = rangeIterator.next();
      blockToSubKeyRangeMap = router.resolveBlocks(keyRange);
    } else {
      LOG.log(Level.SEVERE, "Invalid operation");
      resultAggregator.submitLocalResult(operation, Collections.EMPTY_MAP, Collections.EMPTY_LIST);
      return;
    }
    while (rangeIterator.hasNext()) {
      final LongRange keyRange = rangeIterator.next();
      // blockIds for blockToSubKeyRangeMap never overlap
      blockToSubKeyRangeMap.putAll(router.resolveBlocks(keyRange));
    }

    final int numSubOps = blockToSubKeyRangeMap.size();
    resultAggregator.registerOp(operation, numSubOps);

    // enqueue operation
    if (!blockToSubKeyRangeMap.isEmpty()) {
      // progress only when the store has blocks for a data type of the operation
      if (typeToBlocks.containsKey(dataType)) {
        enqueueOperation(operation, blockToSubKeyRangeMap);
      } else {
        // otherwise, initialize blocks and continue the operation only when it's operation type is PUT
        if (operation.getOperationType() == DataOpType.PUT) {
          initBlocks(dataType);
          enqueueOperation(operation, blockToSubKeyRangeMap);
        }
      }
    }
  }

  /**
   * Enqueues local sub operations to {@code subOperationQueue}.
   */
  private void enqueueOperation(final LongKeyOperation operation,
                                final Map<Integer, LongRange> blockToKeyRangeMap) {
    for (final Map.Entry<Integer, LongRange> blockToSubKeyRange : blockToKeyRangeMap.entrySet()) {
      final int blockId = blockToSubKeyRange.getKey();
      final LongRange subKeyRange = blockToSubKeyRange.getValue();

      try {
        subOperationQueue.put(new Tuple3<>(operation, subKeyRange, blockId));
      } catch (final InterruptedException e) {
        LOG.log(Level.SEVERE, "Enqueue failed with InterruptedException", e);
      }
    }
  }

  /**
   * Executes an operation requested from a local client.
   */
  private <T> void executeOperation(final LongKeyOperation<T> operation) {

    // route key ranges of the operation
    final List<LongRange> dataKeyRanges = operation.getDataKeyRanges();
    final Pair<Map<Integer, LongRange>, Map<String, List<LongRange>>> routingResult =
        router.route(dataKeyRanges);
    final Map<Integer, LongRange> localBlockToSubKeyRangeMap = routingResult.getFirst();
    final Map<String, List<LongRange>> remoteEvalToSubKeyRangesMap = routingResult.getSecond();

    final int numSubOps = remoteEvalToSubKeyRangesMap.size() + 1; // +1 for local operation
    resultAggregator.registerOp(operation, numSubOps);

    // send remote operations first and execute local operations
    sendOperationToRemoteStores(operation, remoteEvalToSubKeyRangesMap);
    final Map<Long, T> localOutputData = executeLocalOperation(operation, localBlockToSubKeyRangeMap);

    // submit the local result and wait until all remote operations complete
    resultAggregator.submitLocalResult(operation, localOutputData, Collections.EMPTY_LIST);
  }

  /**
   * Executes sub local operations directly, not via queueing.
   */
  private <T> Map<Long, T> executeLocalOperation(final LongKeyOperation<T> operation,
                                                 final Map<Integer, LongRange> blockToSubKeyRangeList) {
    if (blockToSubKeyRangeList.isEmpty()) {
      return Collections.EMPTY_MAP;
    }

    final String dataType = operation.getDataType();

    // if there's no initialized block for a data type of the operation,
    // initialize blocks and continue the operation only when it's operation type is PUT,
    // else do not execute the operation.
    if (!typeToBlocks.containsKey(dataType)) {
      if (operation.getOperationType() == DataOpType.PUT) {
        initBlocks(dataType);
      } else {
        return Collections.EMPTY_MAP;
      }
    }

    final Map<Integer, Block> blocks = typeToBlocks.get(operation.getDataType());

    final Map<Long, T> outputData;
    final Iterator<Map.Entry<Integer, LongRange>> blockToSubKeyRangeIterator =
        blockToSubKeyRangeList.entrySet().iterator();

    // first execute a head range to reuse the returned map object for a return map
    if (blockToSubKeyRangeIterator.hasNext()) {
      final Map.Entry<Integer, LongRange> blockToSubKeyRange = blockToSubKeyRangeIterator.next();
      final Block block = blocks.get(blockToSubKeyRange.getKey());
      final LongRange subKeyRange = blockToSubKeyRange.getValue();

      outputData = block.executeSubOperation(operation, subKeyRange);
    } else {
      return Collections.EMPTY_MAP;
    }

    // execute remaining ranges if exist
    while (blockToSubKeyRangeIterator.hasNext()) {
      final Map.Entry<Integer, LongRange> blockToSubKeyRange = blockToSubKeyRangeIterator.next();
      final Block block = blocks.get(blockToSubKeyRange.getKey());
      final LongRange subKeyRange = blockToSubKeyRange.getValue();

      final Map<Long, T> partialOutput = block.executeSubOperation(operation, subKeyRange);
      outputData.putAll(partialOutput);
    }

    return outputData;
  }

  /**
   * Sends sub operations to target remote evaluators.
   */
  private <V> void sendOperationToRemoteStores(final LongKeyOperation<V> operation,
                                               final Map<String, List<LongRange>> evalToSubKeyRangesMap) {
    if (evalToSubKeyRangesMap.isEmpty()) {
      return;
    }

    final Codec codec = serializer.getCodec(operation.getDataType());

    // send sub operations to all remote stores that owns partial range of the main operation (RemoteOpMsg)
    for (final Map.Entry<String, List<LongRange>> evalToSubKeyRange : evalToSubKeyRangesMap.entrySet()) {
      try (final TraceScope traceScope = Trace.startSpan("SEND_REMOTE_OP")) {
        final TraceInfo traceInfo = TraceInfo.fromSpan(traceScope.getSpan());

        final String targetEvalId = evalToSubKeyRange.getKey();
        final List<LongRange> keyRangeList = evalToSubKeyRange.getValue();

        final List<UnitIdPair> dataKVPairList;
        if (operation.getOperationType() == DataOpType.PUT) {
          final NavigableMap<Long, V> keyValueMap = operation.getDataKeyValueMap().get();

          dataKVPairList = new LinkedList<>();

          for (final LongRange keyRange : keyRangeList) {

            // extract range-matching entries from the input data map
            final Map<Long, V> subMap = keyValueMap.subMap(keyRange.getMinimumLong(), true,
                keyRange.getMaximumLong(), true);

            // encode data values and put them into dataKVPairList
            for (final Map.Entry<Long, V> dataKVPair : subMap.entrySet()) {
              final ByteBuffer encodedData = ByteBuffer.wrap(codec.encode(dataKVPair.getValue()));
              dataKVPairList.add(new UnitIdPair(encodedData, dataKVPair.getKey()));
            }
          }
        } else {
          // For GET and REMOVE operations, set dataKVPairList as an empty list
          dataKVPairList = Collections.EMPTY_LIST;
        }

        msgSender.get().sendRemoteOpMsg(operation.getOrigEvalId().get(), targetEvalId,
            operation.getOperationType(), operation.getDataType(), keyRangeList, dataKVPairList,
            operation.getOperationId(), traceInfo);
      }
    }
  }

  @Override
  public <V> Pair<Long, Boolean> put(final String dataType, final Long id, final V value) {
    if (value == null) {
      return new Pair<>(id, false);
    }

    final String operationId = Long.toString(operationIdCounter.getAndIncrement());

    final LongKeyOperation<V> operation = new LongKeyOperation<>(Optional.<String>empty(), operationId, DataOpType.PUT,
        dataType, id, Optional.of(value));

    executeOperation(operation);

    return new Pair<>(id, operation.getFailedRanges().isEmpty());
  }

  @Override
  public <V> Map<Long, Boolean> putList(final String dataType, final List<Long> ids, final List<V> values) {
    if (ids.size() != values.size()) {
      throw new RuntimeException("Different list sizes: ids " + ids.size() + ", values " + values.size());
    }

    final String operationId = Long.toString(operationIdCounter.getAndIncrement());

    final List<LongRange> longRangeSet = new ArrayList<>(LongRangeUtils.generateDenseLongRanges(new TreeSet<>(ids)));

    final NavigableMap<Long, V> dataKeyValueMap = new TreeMap<>();
    for (int idx = 0; idx < ids.size(); idx++) {
      dataKeyValueMap.put(ids.get(idx), values.get(idx));
    }

    final LongKeyOperation<V> operation = new LongKeyOperation<>(Optional.<String>empty(), operationId, DataOpType.PUT,
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
      // skip keys that is smaller than the left end of range
      if (range.getMinimumLong() > key) {
        resultMap.put(key, true);
        // go to next key
        continue;
      }

      // skip ranges whose right end is smaller than the key
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
      resultMap.put(key, false);
    }

    // put all remaining keys to resultMap
    for (; keyIdx < inputKeys.size(); keyIdx++) {
      resultMap.put(inputKeys.get(keyIdx), true);
    }
    return resultMap;
  }

  @Override
  public <V> Pair<Long, V> get(final String dataType, final Long id) {

    final String operationId = Long.toString(operationIdCounter.getAndIncrement());

    final LongKeyOperation<V> operation = new LongKeyOperation<>(Optional.<String>empty(), operationId, DataOpType.GET,
        dataType, id, Optional.<V>empty());

    executeOperation(operation);

    final V outputData = operation.getOutputData().get(id);

    return outputData == null ? null : new Pair<>(id, outputData);
  }

  @Override
  public <V> Map<Long, V> getAll(final String dataType) {
    if (!typeToBlocks.containsKey(dataType)) {
      return Collections.EMPTY_MAP;
    }

    final Map<Long, V> result;
    final Collection<Block> blocks = typeToBlocks.get(dataType).values();

    final Iterator<Block> blockIterator = blocks.iterator();

    // first execute on a head block to reuse the returned map object for a return map
    if (blockIterator.hasNext()) {
      final Block block = blockIterator.next();
      result = block.getAll();
    } else {
      return Collections.EMPTY_MAP;
    }

    // execute on remaining blocks if exist
    while (blockIterator.hasNext()) {
      final Block block = blockIterator.next();
      // huge memory pressure may happen here
      result.putAll((Map<Long, V>) block.getAll());
    }

    return result;
  }

  @Override
  public <V> Map<Long, V> getRange(final String dataType, final Long startId, final Long endId) {

    final String operationId = Long.toString(operationIdCounter.getAndIncrement());

    final LongKeyOperation<V> operation = new LongKeyOperation<>(Optional.<String>empty(), operationId, DataOpType.GET,
        dataType, new LongRange(startId, endId), Optional.<NavigableMap<Long, V>>empty());

    executeOperation(operation);

    return operation.getOutputData();
  }

  @Override
  public <V> Pair<Long, V> remove(final String dataType, final Long id) {

    final String operationId = Long.toString(operationIdCounter.getAndIncrement());
    final LongKeyOperation<V> operation = new LongKeyOperation<>(Optional.<String>empty(), operationId,
        DataOpType.REMOVE, dataType, id, Optional.<V>empty());

    executeOperation(operation);

    final V outputData = operation.getOutputData().get(id);

    return outputData == null ? null : new Pair<>(id, outputData);
  }

  @Override
  public <V> Map<Long, V> removeAll(final String dataType) {
    if (!typeToBlocks.containsKey(dataType)) {
      return Collections.EMPTY_MAP;
    }

    final Map<Long, V> result;
    final Collection<Block> blocks = typeToBlocks.get(dataType).values();


    final Iterator<Block> blockIterator = blocks.iterator();

    // first execute on a head block to reuse the returned map object for a return map
    if (blockIterator.hasNext()) {
      final Block block = blockIterator.next();
      result = block.removeAll();
    } else {
      return Collections.EMPTY_MAP;
    }

    // execute on remaining blocks if exist
    while (blockIterator.hasNext()) {
      final Block block = blockIterator.next();
      // huge memory pressure may happen here
      result.putAll((Map<Long, V>) block.removeAll());
    }

    return result;
  }

  @Override
  public <V> Map<Long, V> removeRange(final String dataType, final Long startId, final Long endId) {

    final String operationId = Long.toString(operationIdCounter.getAndIncrement());

    final LongKeyOperation<V> operation = new LongKeyOperation<>(Optional.<String>empty(), operationId,
        DataOpType.REMOVE, dataType, new LongRange(startId, endId), Optional.<NavigableMap<Long, V>>empty());

    executeOperation(operation);

    return operation.getOutputData();
  }

  @Override
  public Set<String> getDataTypes() {
    return new HashSet<>(typeToBlocks.keySet());
  }

  @Override
  public int getNumUnits(final String dataType) {
    if (!typeToBlocks.containsKey(dataType)) {
      return 0;
    }

    int numUnits = 0;
    final Collection<Block> blocks = typeToBlocks.get(dataType).values();
    for (final Block block : blocks) {
      numUnits += block.getNumUnits();
    }
    return numUnits;
  }
}
