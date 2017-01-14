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
package edu.snu.cay.services.em.evaluator.impl.rangekey;

import edu.snu.cay.services.em.avro.DataOpType;
import edu.snu.cay.services.em.common.parameters.NumStoreThreads;
import edu.snu.cay.services.em.evaluator.api.*;
import edu.snu.cay.services.em.evaluator.impl.BlockStore;
import edu.snu.cay.services.em.evaluator.impl.OwnershipCache;
import edu.snu.cay.utils.LongRangeUtils;
import edu.snu.cay.utils.Tuple3;
import edu.snu.cay.utils.trace.HTrace;
import org.apache.commons.lang.math.LongRange;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.Optional;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A {@code MemoryStore} implementation for a key of long type, supporting range operations.
 * It routes operations to local {@link BlockStore} or remote through {@link RemoteOpHandler}
 * based on the routing result from {@link OwnershipCache}.
 * Assuming EM applications always need to instantiate this class, HTrace initialization is done in the constructor.
 */
@EvaluatorSide
@Private
public final class MemoryStoreImpl implements RemoteAccessibleMemoryStore<Long> {
  private static final Logger LOG = Logger.getLogger(MemoryStoreImpl.class.getName());

  private static final int QUEUE_SIZE = 1024;
  private static final int QUEUE_TIMEOUT_MS = 3000;

  private final OwnershipCache ownershipCache;
  private final BlockResolver<Long> blockResolver;
  private final RemoteOpHandlerImpl<Long> remoteOpHandlerImpl;
  private final BlockStore blockStore;

  /**
   * A counter for issuing ids for operations requested from local clients.
   */
  private final AtomicLong operationIdCounter = new AtomicLong(0);

  /**
   * A queue for operations requested from remote clients.
   * Its element is composed of a operation, sub key ranges, and a corresponding block id.
   */
  private final BlockingQueue<Tuple3<RangeKeyOperation, List<Pair<Long, Long>>, Integer>> subOperationQueue
      = new ArrayBlockingQueue<>(QUEUE_SIZE);

  @Inject
  private MemoryStoreImpl(final HTrace hTrace,
                          final OwnershipCache ownershipCache,
                          final BlockResolver<Long> blockResolver,
                          final RemoteOpHandlerImpl<Long> remoteOpHandlerImpl,
                          final BlockStore blockStore,
                          @Parameter(NumStoreThreads.class) final int numStoreThreads) {
    hTrace.initialize();
    this.ownershipCache = ownershipCache;
    this.blockResolver = blockResolver;
    this.remoteOpHandlerImpl = remoteOpHandlerImpl;
    this.blockStore = blockStore;
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

  @Override
  public boolean registerBlockUpdateListener(final BlockUpdateListener listener) {
    return blockStore.registerBlockUpdateListener(listener);
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
          final Tuple3<RangeKeyOperation, List<Pair<Long, Long>>, Integer> subOperation =
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

    private void handleSubOperation(final Tuple3<RangeKeyOperation, List<Pair<Long, Long>>, Integer> subOperation) {
      final RangeKeyOperation operation = subOperation.getFirst();
      final List<Pair<Long, Long>> subKeyRanges = subOperation.getSecond();
      final int blockId = subOperation.getThird();

      LOG.log(Level.FINEST, "Poll op: [OpId: {0}, origId: {1}, block: {2}]]",
          new Object[]{operation.getOpId(), operation.getOrigEvalId().get(), blockId});

      final Tuple<Optional<String>, Lock> remoteEvalIdWithLock = ownershipCache.resolveEvalWithLock(blockId);
      try {
        final Optional<String> remoteEvalIdOptional = remoteEvalIdWithLock.getKey();
        final boolean isLocal = !remoteEvalIdOptional.isPresent();
        if (isLocal) {
          final BlockImpl block = (BlockImpl) blockStore.get(blockId);
          final Map<Long, Object> result = block.executeSubOperation(operation, subKeyRanges);
          submitLocalResult(operation, result, Collections.emptyList());
        } else {
          LOG.log(Level.WARNING,
              "Failed to execute operation {0} requested by remote store {2}. This store was considered as the owner" +
                  " of block {1} by store {2}, but the local ownershipCache assumes store {3} is the owner",
              new Object[]{operation.getOpId(), blockId, operation.getOrigEvalId().get(), remoteEvalIdOptional.get()});

          // treat remote ranges as failed ranges, because we do not allow more than one hop in remote access
          final List<Pair<Long, Long>> failedRanges = new ArrayList<>(1);
          for (final Pair<Long, Long> subKeyRange : subKeyRanges) {
            failedRanges.add(new Pair<>(subKeyRange.getFirst(), subKeyRange.getSecond()));
          }
          submitLocalResult(operation, Collections.emptyMap(), failedRanges);
        }
      } finally {
        final Lock readLock = remoteEvalIdWithLock.getValue();
        readLock.unlock();
      }
    }
  }

  /**
   * Handles operations requested from a remote client.
   */
  @Override
  public void onNext(final DataOperation dataOperation) {
    final RangeKeyOperation<Long, Object> operation = (RangeKeyOperation<Long, Object>) dataOperation;

    final Map<Integer, List<Pair<Long, Long>>> blockToSubKeyRangesMap =
        splitIntoSubKeyRanges(operation.getDataKeyRanges());

    // cannot resolve any block. invalid data keys
    if (blockToSubKeyRangesMap.isEmpty()) {
      // TODO #421: should handle fail case different from empty case
      submitLocalResult(operation, Collections.emptyMap(), operation.getDataKeyRanges());
      LOG.log(Level.SEVERE, "Failed Op [Id: {0}, origId: {1}]",
          new Object[]{operation.getOpId(), operation.getOrigEvalId().get()});
      return;
    }

    enqueueOperation(operation, blockToSubKeyRangesMap);
  }

  /**
   * Enqueues sub operations requested from a remote client to {@code subOperationQueue}.
   * The enqueued operations are executed by {@code OperationThread}s.
   */
  private void enqueueOperation(final RangeKeyOperation operation,
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

  private Map<Integer, List<Pair<Long, Long>>> splitIntoSubKeyRanges(final List<Pair<Long, Long>> dataKeyRanges) {
    // split into ranges per block
    final Map<Integer, List<Pair<Long, Long>>> blockToSubKeyRangesMap = new HashMap<>();

    for (final Pair<Long, Long> keyRange : dataKeyRanges) {
      final Map<Integer, Pair<Long, Long>> blockToSubKeyRangeMap =
          blockResolver.resolveBlocksForOrderedKeys(keyRange.getFirst(), keyRange.getSecond());

      for (final Map.Entry<Integer, Pair<Long, Long>> blockToSubKeyRange : blockToSubKeyRangeMap.entrySet()) {
        final int blockId = blockToSubKeyRange.getKey();
        final Pair<Long, Long> subKeyRange = blockToSubKeyRange.getValue();

        blockToSubKeyRangesMap.computeIfAbsent(blockId, integer -> new LinkedList<>());

        final List<Pair<Long, Long>> subKeyRangeList = blockToSubKeyRangesMap.get(blockId);
        subKeyRangeList.add(subKeyRange);
      }
    }

    return blockToSubKeyRangesMap;
  }

  /**
   * Executes an operation requested from a local client.
   */
  private <V> void executeOperation(final RangeKeyOperation<Long, V> operation) {

    final Map<Integer, List<Pair<Long, Long>>> blockToSubKeyRangesMap =
        splitIntoSubKeyRanges(operation.getDataKeyRanges());

    final Map<Integer, List<Pair<Long, Long>>> localBlockToSubKeyRangesMap = new HashMap<>();
    final Map<String, List<Pair<Long, Long>>> remoteEvalToSubKeyRangesMap = new HashMap<>();

    // classify sub-ranges into remote and local
    for (final Map.Entry<Integer, List<Pair<Long, Long>>> entry : blockToSubKeyRangesMap.entrySet()) {
      final int blockId = entry.getKey();
      final List<Pair<Long, Long>> rangeList = entry.getValue();
      final Optional<String> remoteEvalIdOptional = ownershipCache.resolveEval(blockId);

      if (remoteEvalIdOptional.isPresent()) { // remote blocks
        // aggregate sub key ranges per evaluator
        final String remoteEvalId = remoteEvalIdOptional.get();
        if (remoteEvalToSubKeyRangesMap.containsKey(remoteEvalId)) {
          remoteEvalToSubKeyRangesMap.get(remoteEvalId).addAll(rangeList);
        } else {
          remoteEvalToSubKeyRangesMap.put(remoteEvalId, rangeList);
        }
      } else { // local blocks
        // aggregate sub key ranges per block
        if (localBlockToSubKeyRangesMap.containsKey(blockId)) {
          localBlockToSubKeyRangesMap.get(blockId).addAll(rangeList);
        }
        localBlockToSubKeyRangesMap.put(blockId, rangeList);
      }
    }

    final int numSubOps = remoteEvalToSubKeyRangesMap.size() + 1; // +1 for local operation
    operation.setNumSubOps(numSubOps);

    LOG.log(Level.FINE, "Execute operation requested from local client. OpId: {0}, OpType: {1}, numSubOps: {2}",
        new Object[]{operation.getOpId(), operation.getOpType(), numSubOps});

    // execute local operation and submit the result
    final Map<Long, V> localOutputData = executeLocalOperation(operation, localBlockToSubKeyRangesMap);
    submitLocalResult(operation, localOutputData, Collections.emptyList());

    // send remote operations and wait until all remote operations complete
    remoteOpHandlerImpl.sendOpToRemoteStores(operation, remoteEvalToSubKeyRangesMap);
  }

  /**
   * Executes sub local operations directly, not via queueing.
   */
  private <V> Map<Long, V> executeLocalOperation(final RangeKeyOperation<Long, V> operation,
                                                 final Map<Integer, List<Pair<Long, Long>>> blockToSubKeyRangesMap) {
    if (blockToSubKeyRangesMap.isEmpty()) {
      return Collections.emptyMap();
    }

    final Map<Long, V> outputData;
    final Iterator<Map.Entry<Integer, List<Pair<Long, Long>>>> blockToSubKeyRangesIterator =
        blockToSubKeyRangesMap.entrySet().iterator();

    // first execute a head range to reuse the returned map object for a return map
    if (blockToSubKeyRangesIterator.hasNext()) {
      final Map.Entry<Integer, List<Pair<Long, Long>>> blockToSubKeyRanges = blockToSubKeyRangesIterator.next();

      final int blockId = blockToSubKeyRanges.getKey();
      final List<Pair<Long, Long>> subKeyRanges = blockToSubKeyRanges.getValue();

      outputData = executeLocalSubOperation(operation, blockId, subKeyRanges);
    } else {
      return Collections.emptyMap();
    }

    // execute remaining ranges if exist
    while (blockToSubKeyRangesIterator.hasNext()) {
      final Map<Long, V> partialOutput;
      final Map.Entry<Integer, List<Pair<Long, Long>>> blockToSubKeyRanges = blockToSubKeyRangesIterator.next();

      final int blockId = blockToSubKeyRanges.getKey();
      final List<Pair<Long, Long>> subKeyRanges = blockToSubKeyRanges.getValue();
      partialOutput = executeLocalSubOperation(operation, blockId, subKeyRanges);

      outputData.putAll(partialOutput);
    }

    return outputData;
  }

  private <V> Map<Long, V> executeLocalSubOperation(final RangeKeyOperation<Long, V> operation,
                                                    final int blockId, final List<Pair<Long, Long>> subKeyRanges) {
    final Map<Long, V> outputData;
    final Lock readLock = ownershipCache.resolveEvalWithLock(blockId).getValue();
    try {
      final BlockImpl block = (BlockImpl) blockStore.get(blockId);
      outputData = block.executeSubOperation(operation, subKeyRanges);
    } finally {
      readLock.unlock();
    }
    return outputData;
  }

  /**
   * Handles the result of data operation processed by local memory store.
   * It waits until all sub operations are finished and their outputs are fully aggregated.
   */
  private <V> void submitLocalResult(final RangeKeyOperation<Long, V> operation, final Map<Long, V> localOutput,
                                     final List<Pair<Long, Long>> failedRanges) {
    final int numRemainingSubOps = operation.commitResult(localOutput, failedRanges);

    LOG.log(Level.FINE, "Local sub operation is finished. OpId: {0}, numRemainingSubOps: {1}",
        new Object[]{operation.getOpId(), numRemainingSubOps});

    if (!operation.isFromLocalClient() && numRemainingSubOps == 0) {
      remoteOpHandlerImpl.sendResultToOrigin(operation);
    }
  }

  @Override
  public <V> Pair<Long, Boolean> put(final Long id, @Nonnull final V value) {
    final String operationId = Long.toString(operationIdCounter.getAndIncrement());

    final RangeKeyOperation<Long, V> operation = new RangeKeyOperationImpl<>(Optional.<String>empty(), operationId,
        DataOpType.PUT, id, Optional.of(value));

    executeOperation(operation);

    return new Pair<>(id, operation.getFailedKeyRanges().isEmpty());
  }

  @Override
  public <V> Map<Long, Boolean> putList(final List<Long> ids, final List<V> values) {
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

    final RangeKeyOperation<Long, V> operation = new RangeKeyOperationImpl<>(Optional.<String>empty(), operationId,
        DataOpType.PUT, keyRangeList, Optional.of(dataKeyValueMap));

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
  public <V> Pair<Long, V> get(final Long id) {

    final String operationId = Long.toString(operationIdCounter.getAndIncrement());

    final RangeKeyOperation<Long, V> operation = new RangeKeyOperationImpl<>(Optional.<String>empty(), operationId,
        DataOpType.GET, id, Optional.<V>empty());

    executeOperation(operation);

    final V outputData = operation.getOutputData().get(id);

    return outputData == null ? null : new Pair<>(id, outputData);
  }

  @Override
  public <V> Map<Long, V> getAll() {
    final Map<Long, V> result;

    final List<Integer> localBlockIds = ownershipCache.getCurrentLocalBlockIds();
    final Iterator<Integer> blockIdIterator = localBlockIds.iterator();

    // first execute on a head block to reuse the returned map object for a return map
    if (blockIdIterator.hasNext()) {
      final Block block = blockStore.get(blockIdIterator.next());
      result = block.getAll();
    } else {
      return Collections.emptyMap();
    }

    // execute on remaining blocks if exist
    while (blockIdIterator.hasNext()) {
      final Block block = blockStore.get(blockIdIterator.next());
      // huge memory pressure may happen here
      result.putAll(block.getAll());
    }

    return result;
  }

  @Override
  public <V> Map<Long, V> getRange(final Long startId, final Long endId) {

    final String operationId = Long.toString(operationIdCounter.getAndIncrement());

    final RangeKeyOperation<Long, V> operation = new RangeKeyOperationImpl<>(Optional.<String>empty(), operationId,
        DataOpType.GET, new Pair<>(startId, endId), Optional.<NavigableMap<Long, V>>empty());

    executeOperation(operation);

    return operation.getOutputData();
  }

  @Override
  public <V> Pair<Long, V> update(final Long id, final V deltaValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <V> Pair<Long, V> remove(final Long id) {

    final String operationId = Long.toString(operationIdCounter.getAndIncrement());
    final RangeKeyOperation<Long, V> operation = new RangeKeyOperationImpl<>(Optional.<String>empty(), operationId,
        DataOpType.REMOVE, id, Optional.<V>empty());

    executeOperation(operation);

    final V outputData = operation.getOutputData().get(id);

    return outputData == null ? null : new Pair<>(id, outputData);
  }

  @Override
  public <V> Map<Long, V> removeAll() {
    final Map<Long, V> result;

    final List<Integer> localBlockIds = ownershipCache.getCurrentLocalBlockIds();
    final Iterator<Integer> blockIdIterator = localBlockIds.iterator();

    // first execute on a head block to reuse the returned map object for a return map
    if (blockIdIterator.hasNext()) {
      final Block block = blockStore.get(blockIdIterator.next());
      result = block.removeAll();
    } else {
      return Collections.emptyMap();
    }

    // execute on remaining blocks if exist
    while (blockIdIterator.hasNext()) {
      final Block block = blockStore.get(blockIdIterator.next());
      // huge memory pressure may happen here
      result.putAll(block.removeAll());
    }

    return result;
  }

  @Override
  public <V> Map<Long, V> removeRange(final Long startId, final Long endId) {

    final String operationId = Long.toString(operationIdCounter.getAndIncrement());

    final RangeKeyOperation<Long, V> operation = new RangeKeyOperationImpl<>(Optional.<String>empty(), operationId,
        DataOpType.REMOVE, new Pair<>(startId, endId), Optional.<NavigableMap<Long, V>>empty());

    executeOperation(operation);

    return operation.getOutputData();
  }

  @Override
  public int getNumBlocks() {
    return blockStore.getNumBlocks();
  }

  @Override
  public Optional<String> resolveEval(final Long key) {
    final int blockId = blockResolver.resolveBlock(key);
    return ownershipCache.resolveEval(blockId);
  }
}
