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
package edu.snu.cay.services.em.evaluator.impl.range;

import edu.snu.cay.services.em.avro.DataOpType;
import edu.snu.cay.services.em.common.parameters.NumStoreThreads;
import edu.snu.cay.services.em.evaluator.api.BlockResolver;
import edu.snu.cay.services.em.evaluator.api.DataOperation;
import edu.snu.cay.services.em.evaluator.api.RangeOperation;
import edu.snu.cay.services.em.evaluator.api.RemoteAccessibleMemoryStore;
import edu.snu.cay.services.em.evaluator.impl.OperationRouter;
import edu.snu.cay.utils.LongRangeUtils;
import edu.snu.cay.utils.Tuple3;
import edu.snu.cay.utils.trace.HTrace;
import org.apache.commons.lang.math.LongRange;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A {@code MemoryStore} implementation for a key of long type, supporting range operations.
 * All data of one data type is stored in multiple Blocks embedding a {@code TreeMap}, ordered by data ids.
 * These Blocks are then maintained as values of one big {@code HashMap}, which uses the data types as keys.
 * Each Block has {@code ReentrantReadWriteLock} for synchronization between {@code get}, {@code put},
 * and {@code remove} operations within Block.
 * Assuming EM applications always need to instantiate this class, HTrace initialization is done in the constructor.
 */
@EvaluatorSide
@Private
public final class MemoryStoreImpl implements RemoteAccessibleMemoryStore<Long> {
  private static final Logger LOG = Logger.getLogger(MemoryStoreImpl.class.getName());

  private static final int QUEUE_SIZE = 1024;
  private static final int QUEUE_TIMEOUT_MS = 3000;

  /**
   * This map uses data types, represented as strings, for keys and inner map for values.
   * Each inner map serves as a collection of data of the same data type, which is compose of multiple Blocks.
   * Each inner map maintains mapping between a Block id and Block itself.
   */
  private final Map<String, Map<Integer, Block>> typeToBlocks = new HashMap<>();

  @GuardedBy("routerLock")
  private final OperationRouter<Long> router;
  private final BlockResolver<Long> blockResolver;
  private final RemoteOpHandler<Long> remoteOpHandler;

  private final ReadWriteLock routerLock = new ReentrantReadWriteLock(true);

  /**
   * A counter for issuing ids for operations requested from local clients.
   */
  private final AtomicLong operationIdCounter = new AtomicLong(0);

  /**
   * A queue for operations requested from remote clients.
   * Its element is composed of a operation, sub key ranges, and a corresponding block id.
   */
  private final BlockingQueue<Tuple3<RangeOperation, List<Pair<Long, Long>>, Integer>> subOperationQueue
      = new ArrayBlockingQueue<>(QUEUE_SIZE);

  @Inject
  private MemoryStoreImpl(final HTrace hTrace,
                          final OperationRouter<Long> router,
                          final BlockResolver<Long> blockResolver,
                          final RemoteOpHandler<Long> remoteOpHandler,
                          @Parameter(NumStoreThreads.class) final int numStoreThreads) {
    hTrace.initialize();
    this.router = router;
    this.blockResolver = blockResolver;
    this.remoteOpHandler = remoteOpHandler;
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
    // We don't need to lock router because this method is already synchronized.
    for (final int blockId : router.getInitialLocalBlockIds()) {
      initialBlocks.put(blockId, new Block());
    }

    // must put initialBlocks into typeToBlocks after completely initialize it
    typeToBlocks.put(dataType, initialBlocks);
  }

  @Override
  public void updateOwnership(final String dataType, final int blockId, final int oldOwnerId, final int newOwnerId) {
    routerLock.writeLock().lock();
    try {
      router.updateOwnership(blockId, oldOwnerId, newOwnerId);
    } finally {
      routerLock.writeLock().unlock();
    }
  }

  @Override
  public void putBlock(final String dataType, final int blockId, final Map<Long, Object> data) {
    final Map<Integer, Block> blocks = typeToBlocks.get(dataType);
    if (null == blocks) {
      // If the blocks of the type have not been initialized, then create the blocks.
      initBlocks(dataType);
    } else if (blocks.containsKey(blockId)) {
      throw new RuntimeException("Block with id " + blockId + " already exists.");
    } else {
      final Block block = new Block();
      block.subDataMap.putAll(data);
      blocks.put(blockId, block);
    }
  }

  @Override
  public Map<Long, Object> getBlock(final String dataType, final int blockId) {
    final Map<Integer, Block> blocks = typeToBlocks.get(dataType);
    if (null == blocks) {
      throw new RuntimeException("Data type " + dataType + " does not exist.");
    }

    final Block block = blocks.get(blockId);
    if (null == block) {
      throw new RuntimeException("Block with id " + blockId + " does not exist.");
    }

    return block.getAll();
  }

  @Override
  public void removeBlock(final String dataType, final int blockId) {
    final Map<Integer, Block> blocks = typeToBlocks.get(dataType);
    if (null == blocks) {
      throw new RuntimeException("Data type " + dataType + " does not exist.");
    }

    final Block block = blocks.remove(blockId);
    if (null == block) {
      throw new RuntimeException("Block with id " + blockId + " does not exist.");
    }
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
          final Tuple3<RangeOperation, List<Pair<Long, Long>>, Integer> subOperation =
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

    private void handleSubOperation(final Tuple3<RangeOperation, List<Pair<Long, Long>>, Integer> subOperation) {
      final RangeOperation operation = subOperation.getFirst();
      final List<Pair<Long, Long>> subKeyRanges = subOperation.getSecond();
      final int blockId = subOperation.getThird();

      LOG.log(Level.FINEST, "Poll op: [OpId: {0}, origId: {1}, block: {2}]]",
          new Object[]{operation.getOpId(), operation.getOrigEvalId().get(), blockId});

      routerLock.readLock().lock();
      try {
        final Optional<String> remoteEvalId = router.resolveEval(blockId);
        final boolean isLocal = !remoteEvalId.isPresent();
        if (isLocal) {
          final Block block = typeToBlocks.get(operation.getDataType()).get(blockId);
          final Map<Long, Object> result = block.executeSubOperation(operation, subKeyRanges);
          submitLocalResult(operation, result, Collections.EMPTY_LIST);
        } else {
          LOG.log(Level.WARNING,
              "Fail to execute operation {0} requested by remote store {2}. This store was considered as the owner" +
                  " of block {1} by store {2}, but the local router assumes store {3} is the owner",
              new Object[]{operation.getOpId(), blockId, operation.getOrigEvalId().get(), remoteEvalId.get()});

          // treat remote ranges as failed ranges, because we do not allow more than one hop in remote access
          final List<Pair<Long, Long>> failedRanges = new ArrayList<>(1);
          for (final Pair<Long, Long> subKeyRange : subKeyRanges) {
            failedRanges.add(new Pair<>(subKeyRange.getFirst(), subKeyRange.getSecond()));
          }
          submitLocalResult(operation, Collections.EMPTY_MAP, failedRanges);
        }
      } finally {
        routerLock.readLock().unlock();
      }
    }
  }

  /**
   * Block class that has a {@code subDataMap}, which is an unit of EM's move.
   * Also it's a concurrency unit for data operations because it has a {@code ReadWriteLock},
   * which regulates accesses to {@code subDataMap}.
   */
  private final class Block<V> {
    /**
     * The map serves as a collection of data in a Block.
     * Its implementation {@code TreeMap} is used for guaranteeing log(n) read and write operations, especially
     * {@code getRange()} and {@code removeRange()} which are ranged queries based on the ids.
     */
    private final NavigableMap<Long, V> subDataMap = new TreeMap<>();

    /**
     * A read-write lock for {@code subDataMap} of the block.
     * Let's set fairness option as true to prevent starvation.
     */
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock(true);

    /**
     * Executes sub operation on data keys assigned to this block.
     * All operations both from remote and local clients are executed via this method.
     */
    private Map<Long, V> executeSubOperation(final RangeOperation<Long, V> operation,
                                             final List<Pair<Long, Long>> keyRanges) {
      final DataOpType operationType = operation.getOpType();

      final Map<Long, V> outputData = new HashMap<>();
      switch (operationType) {
      case PUT:
        rwLock.writeLock().lock();
        try {
          final NavigableMap<Long, V> dataKeyValueMap = operation.getDataKVMap().get();
          for (final Pair<Long, Long> keyRange : keyRanges) {
            // extract matching entries from the input kv data map and put it all to subDataMap
            final NavigableMap<Long, V> subMap =
                dataKeyValueMap.subMap(keyRange.getFirst(), true, keyRange.getSecond(), true);
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
          for (final Pair<Long, Long> keyRange : keyRanges) {
            outputData.putAll(subDataMap.subMap(keyRange.getFirst(), true,
                keyRange.getSecond(), true));
          }
        } finally {
          rwLock.readLock().unlock();
        }
        break;
      case REMOVE:
        rwLock.writeLock().lock();
        try {
          for (final Pair<Long, Long> keyRange : keyRanges) {
            outputData.putAll(subDataMap.subMap(keyRange.getFirst(), true,
                keyRange.getSecond(), true));
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

    /**
     * Returns all data in a block.
     * It is for supporting getAll method of MemoryStore.
     */
    private Map<Long, V> getAll() {
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
    private Map<Long, V> removeAll() {
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
     * Returns the number of data in a block.
     * It is for supporting getNumUnits method of MemoryStore.
     */
    private int getNumUnits() {
      return subDataMap.size();
    }
  }

  /**
   * Handles operations requested from a remote client.
   */
  @Override
  public void onNext(final DataOperation dataOperation) {
    final RangeOperation<Long, Object> operation = (RangeOperation<Long, Object>) dataOperation;
    final String dataType = operation.getDataType();

    // split data key ranges into blocks
    final List<Pair<Long, Long>> dataKeyRanges = operation.getDataKeyRanges();
    final Map<Integer, List<Pair<Long, Long>>> blockToSubKeyRangesMap = new HashMap<>();

    for (final Pair<Long, Long> keyRange : dataKeyRanges) {
      final Map<Integer, Pair<Long, Long>> blockToSubKeyRangeMap =
          blockResolver.resolveBlocksForOrderedKeys(keyRange.getFirst(), keyRange.getSecond());

      for (final Map.Entry<Integer, Pair<Long, Long>> blockToSubKeyRange : blockToSubKeyRangeMap.entrySet()) {
        final int blockId = blockToSubKeyRange.getKey();
        final Pair<Long, Long> subKeyRange = blockToSubKeyRange.getValue();

        if (!blockToSubKeyRangesMap.containsKey(blockId)) {
          blockToSubKeyRangesMap.put(blockId, new LinkedList<Pair<Long, Long>>());
        }
        final List<Pair<Long, Long>> subKeyRangeList = blockToSubKeyRangesMap.get(blockId);
        subKeyRangeList.add(subKeyRange);
      }
    }

    // cannot resolve any block. invalid data keys
    if (blockToSubKeyRangesMap.isEmpty()) {
      // TODO #421: should handle fail case different from empty case
      submitLocalResult(operation, Collections.EMPTY_MAP, operation.getDataKeyRanges());
      LOG.log(Level.SEVERE, "Failed Op [Id: {0}, origId: {1}]",
          new Object[]{operation.getOpId(), operation.getOrigEvalId().get()});
      return;
    }

    // progress when there're blocks for dataType
    if (!typeToBlocks.containsKey(dataType)) {
      // nevertheless, for PUT operations initialize blocks and continue the operation
      if (operation.getOpType() == DataOpType.PUT) {
        initBlocks(dataType);
      } else {
        // submit empty result for other types of operations
        submitLocalResult(operation, Collections.EMPTY_MAP, operation.getDataKeyRanges());

        LOG.log(Level.FINEST, "Blocks for the {0}. Send empty result for operation {1} from {2}",
            new Object[]{operation.getDataType(), operation.getOpId(), operation.getOrigEvalId().get()});
        return;
      }
    }

    enqueueOperation(operation, blockToSubKeyRangesMap);
  }

  /**
   * Enqueues sub operations requested from a remote client to {@code subOperationQueue}.
   * The enqueued operations are executed by {@code OperationThread}s.
   */
  private void enqueueOperation(final RangeOperation operation,
                                final Map<Integer, List<Pair<Long, Long>>> blockToKeyRangesMap) {
    final int numSubOps = blockToKeyRangesMap.size();
    operation.setNumSubOps(numSubOps);

    for (final Map.Entry<Integer, List<Pair<Long, Long>>> blockToSubKeyRanges : blockToKeyRangesMap.entrySet()) {
      final int blockId = blockToSubKeyRanges.getKey();
      final List<Pair<Long, Long>> keyRanges = blockToSubKeyRanges.getValue();

      try {
        subOperationQueue.put(new Tuple3<>(operation, keyRanges, blockId));
      } catch (final InterruptedException e) {
        LOG.log(Level.SEVERE, "Enqueue failed with InterruptedException", e);
      }

      LOG.log(Level.FINEST, "Enqueue Op [Id: {0}, block: {1}]",
          new Object[]{operation.getOpId(), blockId});
    }
  }

  /**
   * Executes an operation requested from a local client.
   */
  private <V> void executeOperation(final RangeOperation<Long, V> operation) {

    final List<Pair<Long, Long>> dataKeyRanges = operation.getDataKeyRanges();

    final Map<String, List<Pair<Long, Long>>> remoteEvalToSubKeyRangesMap;

    routerLock.readLock().lock();
    try {
      final Pair<Map<Integer, List<Pair<Long, Long>>>, Map<String, List<Pair<Long, Long>>>> routingResult =
          router.route(dataKeyRanges);
      final Map<Integer, List<Pair<Long, Long>>> localBlockToSubKeyRangesMap = routingResult.getFirst();
      remoteEvalToSubKeyRangesMap = routingResult.getSecond();

      final int numSubOps = remoteEvalToSubKeyRangesMap.size() + 1; // +1 for local operation
      operation.setNumSubOps(numSubOps);

      LOG.log(Level.FINEST, "Execute operation requested from local client. OpId: {0}, OpType: {1}, numSubOps: {2}",
          new Object[]{operation.getOpId(), operation.getOpType(), numSubOps});

      // execute local operation and submit the result
      final Map<Long, V> localOutputData = executeLocalOperation(operation, localBlockToSubKeyRangesMap);
      submitLocalResult(operation, localOutputData, Collections.EMPTY_LIST);
    } finally {
      routerLock.readLock().unlock();
    }

    // send remote operations and wait until all remote operations complete
    remoteOpHandler.sendOpToRemoteStores(operation, remoteEvalToSubKeyRangesMap);
  }

  /**
   * Executes sub local operations directly, not via queueing.
   */
  private <V> Map<Long, V> executeLocalOperation(final RangeOperation<Long, V> operation,
                                                 final Map<Integer, List<Pair<Long, Long>>> blockToSubKeyRangesMap) {
    if (blockToSubKeyRangesMap.isEmpty()) {
      return Collections.EMPTY_MAP;
    }

    final String dataType = operation.getDataType();

    // if there's no initialized block for a data type of the operation,
    // initialize blocks and continue the operation only when it's operation type is PUT.
    // for other types of operation do not execute the operation.
    if (!typeToBlocks.containsKey(dataType)) {
      if (operation.getOpType() == DataOpType.PUT) {
        initBlocks(dataType);
      } else {
        return Collections.EMPTY_MAP;
      }
    }

    final Map<Integer, Block> blocks = typeToBlocks.get(operation.getDataType());

    final Map<Long, V> outputData;
    final Iterator<Map.Entry<Integer, List<Pair<Long, Long>>>> blockToSubKeyRangesIterator =
        blockToSubKeyRangesMap.entrySet().iterator();

    // first execute a head range to reuse the returned map object for a return map
    if (blockToSubKeyRangesIterator.hasNext()) {
      final Map.Entry<Integer, List<Pair<Long, Long>>> blockToSubKeyRanges = blockToSubKeyRangesIterator.next();
      final Block<V> block = blocks.get(blockToSubKeyRanges.getKey());
      final List<Pair<Long, Long>> subKeyRanges = blockToSubKeyRanges.getValue();

      outputData = block.executeSubOperation(operation, subKeyRanges);
    } else {
      return Collections.EMPTY_MAP;
    }

    // execute remaining ranges if exist
    while (blockToSubKeyRangesIterator.hasNext()) {
      final Map.Entry<Integer, List<Pair<Long, Long>>> blockToSubKeyRanges = blockToSubKeyRangesIterator.next();
      final Block<V> block = blocks.get(blockToSubKeyRanges.getKey());
      final List<Pair<Long, Long>> subKeyRanges = blockToSubKeyRanges.getValue();

      final Map<Long, V> partialOutput = block.executeSubOperation(operation, subKeyRanges);
      outputData.putAll(partialOutput);
    }

    return outputData;
  }

  /**
   * Handles the result of data operation processed by local memory store.
   * It waits until all remote sub operations are finished and their outputs are fully aggregated.
   */
  private <V> void submitLocalResult(final RangeOperation<Long, V> operation, final Map<Long, V> localOutput,
                                     final List<Pair<Long, Long>> failedRanges) {
    final int numRemainingSubOps = operation.commitResult(localOutput, failedRanges);

    LOG.log(Level.FINEST, "Local sub operation succeed. OpId: {0}, numRemainingSubOps: {1}",
        new Object[]{operation.getOpId(), numRemainingSubOps});

    if (!operation.isFromLocalClient() && numRemainingSubOps == 0) {
      remoteOpHandler.sendResultToOrigin(operation);
    }
  }

  @Override
  public <V> Pair<Long, Boolean> put(final String dataType, final Long id, @Nonnull final V value) {
    final String operationId = Long.toString(operationIdCounter.getAndIncrement());

    final RangeOperation<Long, V> operation = new RangeOperationImpl<>(Optional.<String>empty(), operationId,
        DataOpType.PUT, dataType, id, Optional.of(value));

    executeOperation(operation);

    return new Pair<>(id, operation.getFailedKeyRanges().isEmpty());
  }

  @Override
  public <V> Map<Long, Boolean> putList(final String dataType, final List<Long> ids, final List<V> values) {
    if (ids.size() != values.size()) {
      throw new RuntimeException("Different list sizes: ids " + ids.size() + ", values " + values.size());
    }

    final String operationId = Long.toString(operationIdCounter.getAndIncrement());

    final List<LongRange> longRangeList = new ArrayList<>(LongRangeUtils.generateDenseLongRanges(new TreeSet<>(ids)));
    final List<Pair<Long, Long>> keyRangeList = new ArrayList<>(longRangeList.size());
    for (final LongRange range : longRangeList) {
      keyRangeList.add(new Pair<>(range.getMinimumLong(), range.getMaximumLong()));
    }

    final NavigableMap<Long, V> dataKeyValueMap = new TreeMap<>();
    for (int idx = 0; idx < ids.size(); idx++) {
      dataKeyValueMap.put(ids.get(idx), values.get(idx));
    }

    final RangeOperation<Long, V> operation = new RangeOperationImpl<>(Optional.<String>empty(), operationId,
        DataOpType.PUT, dataType, keyRangeList, Optional.of(dataKeyValueMap));

    executeOperation(operation);

    return getResultForPutList(ids, operation.getFailedKeyRanges());
  }

  /**
   * Returns a result map for putList operation.
   * The map has entries for all input data keys and corresponding boolean values
   * that are false for failed keys and true for succeeded keys
   */
  private Map<Long, Boolean> getResultForPutList(final List<Long> inputKeys,
                                                 final List<Pair<Long, Long>> failedKeyRangeList) {
    final Map<Long, Boolean> resultMap = new HashMap<>(inputKeys.size());
    if (failedKeyRangeList.isEmpty()) {
      for (final long key : inputKeys) {
        resultMap.put(key, true);
      }
      return resultMap;
    }

    final List<LongRange> failedKeyRanges = new ArrayList<>(failedKeyRangeList.size());
    for (final Pair<Long, Long> range : failedKeyRangeList) {
      failedKeyRanges.add(new LongRange(range.getFirst(), range.getSecond()));
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

    final RangeOperation<Long, V> operation = new RangeOperationImpl<>(Optional.<String>empty(), operationId,
        DataOpType.GET, dataType, id, Optional.<V>empty());

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
      final Block<V> block = blockIterator.next();
      result = block.getAll();
    } else {
      return Collections.EMPTY_MAP;
    }

    // execute on remaining blocks if exist
    while (blockIterator.hasNext()) {
      final Block<V> block = blockIterator.next();
      // huge memory pressure may happen here
      result.putAll(block.getAll());
    }

    return result;
  }

  @Override
  public <V> Map<Long, V> getRange(final String dataType, final Long startId, final Long endId) {

    final String operationId = Long.toString(operationIdCounter.getAndIncrement());

    final RangeOperation<Long, V> operation = new RangeOperationImpl<>(Optional.<String>empty(), operationId,
        DataOpType.GET, dataType, new Pair<>(startId, endId), Optional.<NavigableMap<Long, V>>empty());

    executeOperation(operation);

    return operation.getOutputData();
  }

  @Override
  public <V> Pair<Long, V> remove(final String dataType, final Long id) {

    final String operationId = Long.toString(operationIdCounter.getAndIncrement());
    final RangeOperation<Long, V> operation = new RangeOperationImpl<>(Optional.<String>empty(), operationId,
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
      final Block<V> block = blockIterator.next();
      result = block.removeAll();
    } else {
      return Collections.EMPTY_MAP;
    }

    // execute on remaining blocks if exist
    while (blockIterator.hasNext()) {
      final Block<V> block = blockIterator.next();
      // huge memory pressure may happen here
      result.putAll(block.removeAll());
    }

    return result;
  }

  @Override
  public <V> Map<Long, V> removeRange(final String dataType, final Long startId, final Long endId) {

    final String operationId = Long.toString(operationIdCounter.getAndIncrement());

    final RangeOperation<Long, V> operation = new RangeOperationImpl<>(Optional.<String>empty(), operationId,
        DataOpType.REMOVE, dataType, new Pair<>(startId, endId), Optional.<NavigableMap<Long, V>>empty());

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

  /**
   * Returns the number of local blocks whose type is {@code dataType}.
   * @param dataType a type of data
   * @return the number of blocks of specific type
   */
  public int getNumBlocks(final String dataType) {
    final Map<Integer, Block> blocks = typeToBlocks.get(dataType);
    if (blocks == null) {
      return 0;
    } else {
      return blocks.size();
    }
  }
}
