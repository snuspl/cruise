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
import edu.snu.cay.services.em.evaluator.api.*;
import edu.snu.cay.services.em.evaluator.impl.BlockStore;
import edu.snu.cay.services.em.evaluator.impl.OwnershipCache;
import edu.snu.cay.utils.LongRangeUtils;
import edu.snu.cay.utils.trace.HTrace;
import org.apache.commons.lang.math.LongRange;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.util.Optional;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.*;
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
public final class MemoryStoreImpl implements MemoryStore<Long> {
  private static final Logger LOG = Logger.getLogger(MemoryStoreImpl.class.getName());

  private final OwnershipCache ownershipCache;
  private final BlockResolver<Long> blockResolver;
  private final RangeSplitter<Long> rangeSplitter;
  private final RemoteOpHandlerImpl<Long> remoteOpHandlerImpl;
  private final BlockStore blockStore;

  /**
   * A counter for issuing ids for operations requested from local clients.
   */
  private final AtomicLong operationIdCounter = new AtomicLong(0);

  @Inject
  private MemoryStoreImpl(final HTrace hTrace,
                          final OwnershipCache ownershipCache,
                          final BlockResolver<Long> blockResolver,
                          final RangeSplitter<Long> rangeSplitter,
                          final RemoteOpHandlerImpl<Long> remoteOpHandlerImpl,
                          final BlockStore blockStore) {
    hTrace.initialize();
    this.ownershipCache = ownershipCache;
    this.blockResolver = blockResolver;
    this.rangeSplitter = rangeSplitter;
    this.remoteOpHandlerImpl = remoteOpHandlerImpl;
    this.blockStore = blockStore;
  }

  @Override
  public boolean registerBlockUpdateListener(final BlockUpdateListener listener) {
    return blockStore.registerBlockUpdateListener(listener);
  }
  /**
   * Executes an operation requested from a local client.
   */
  private <V> void executeOperation(final RangeKeyOperation<Long, V> operation) {

    final Map<Integer, List<Pair<Long, Long>>> blockToSubKeyRangesMap =
        rangeSplitter.splitIntoSubKeyRanges(operation.getDataKeyRanges());

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
    final Map<Long, V> localOutputData = executeSubOperation(operation, localBlockToSubKeyRangesMap);
    operation.commitResult(localOutputData, Collections.emptyList());

    // send remote operations and wait until all remote operations complete
    remoteOpHandlerImpl.sendOpToRemoteStores(operation, remoteEvalToSubKeyRangesMap);
  }

  /**
   * Executes sub operations directly, not via queueing.
   */
  private <V> Map<Long, V> executeSubOperation(final RangeKeyOperation<Long, V> operation,
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
    Map<Long, V> result = null;

    // will not care blocks migrated in after this call
    final List<Integer> localBlockIds = ownershipCache.getCurrentLocalBlockIds();

    for (final Integer blockId : localBlockIds) {
      final Tuple<Optional<String>, Lock> remoteEvalIdWithLock = ownershipCache.resolveEvalWithLock(blockId);
      if (!remoteEvalIdWithLock.getKey().isPresent()) { // scan block if it still remains in local
        final Block block = blockStore.get(blockId);

        if (result == null) {
          // reuse the first returned map object
          result = block.getAll();
        } else {
          // huge memory pressure may happen here
          result.putAll(block.getAll());
        }
      }
      remoteEvalIdWithLock.getValue().unlock();
    }

    return result == null ? Collections.emptyMap() : result;
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
    Map<Long, V> result = null;

    // will not care blocks migrated in after this call
    final List<Integer> localBlockIds = ownershipCache.getCurrentLocalBlockIds();

    for (final Integer blockId : localBlockIds) {
      final Tuple<Optional<String>, Lock> remoteEvalIdWithLock = ownershipCache.resolveEvalWithLock(blockId);
      if (!remoteEvalIdWithLock.getKey().isPresent()) { // scan block if it still remains in local
        final Block block = blockStore.get(blockId);

        if (result == null) {
          // reuse the first returned map object
          result = block.removeAll();
        } else {
          // huge memory pressure may happen here
          result.putAll(block.removeAll());
        }
      }
      remoteEvalIdWithLock.getValue().unlock();
    }

    return result == null ? Collections.emptyMap() : result;
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
