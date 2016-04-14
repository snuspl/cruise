/*
 * Copyright (C) 2016 Seoul National University
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

import edu.snu.cay.services.em.common.parameters.MemoryStoreId;
import edu.snu.cay.services.em.common.parameters.NumTotalBlocks;
import edu.snu.cay.services.em.common.parameters.NumInitialEvals;
import edu.snu.cay.services.em.evaluator.api.BlockResolver;
import edu.snu.cay.utils.LongRangeUtils;
import org.apache.commons.lang.math.LongRange;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.Optional;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * OperationRouter that redirects incoming operations on specific data ids to corresponding evaluators.
 */
@Private
public final class OperationRouter {

  private static final Logger LOG = Logger.getLogger(OperationRouter.class.getName());

  private String evalPrefix;

  private final int localStoreId;

  private final BlockResolver blockResolver;

  /**
   * The number of total blocks.
   */
  private final int numTotalBlocks;

  /**
   * The number of initial Evaluators.
   */
  private final int numInitialEvals;

  /**
   * The location of blocks. It keeps just an index of MemoryStores,
   * so prefix should be added to get the Evaluator's endpoint id (See {@link #route(long)}).
   */
  private final int[] blockIdToStoreId;
  private final List<Integer> localBlocks;

  // TODO #380: we have to improve router to provide different routing tables for each dataType.
  @Inject
  private OperationRouter(final BlockResolver blockResolver,
                          @Parameter(NumTotalBlocks.class) final int numTotalBlocks,
                          @Parameter(NumInitialEvals.class) final int numInitialEvals,
                          @Parameter(MemoryStoreId.class) final int memoryStoreId) {
    this.blockResolver = blockResolver;
    this.localStoreId = memoryStoreId;
    this.numTotalBlocks = numTotalBlocks;
    this.numInitialEvals = numInitialEvals;
    this.localBlocks = new ArrayList<>(numTotalBlocks / numInitialEvals + 1); // +1 for remainders
    this.blockIdToStoreId = new int[numTotalBlocks];
    initBlocks();
  }

  private void initBlocks() {
    for (int blockId = localStoreId; blockId < numTotalBlocks; blockId += numInitialEvals) {
      localBlocks.add(blockId);
    }

    // blocks are initially distributed across Evaluators in round-robin.
    for (int blockIdx = 0; blockIdx < numTotalBlocks; blockIdx++) {
      final int storeIdx = blockIdx % numInitialEvals;
      blockIdToStoreId[blockIdx] = storeIdx;
    }
  }

  /**
   * Initialize the router.
   */
  public void initialize(final String endPointId) {
    this.evalPrefix = endPointId.split("-")[0];
    LOG.log(Level.INFO, "Initialize router with localEndPointId: {0}", endPointId);
  }

  /**
   * Decompose a list of key ranges into local ranges and remote ranges.
   * TODO #424: improve and optimize routing for range
   * @param dataKeyRanges a list of key ranges
   * @return a map composed of a block id and a corresponding key range list and
   * a map composed of an endpoint id of remote evaluator and a corresponding key range list.
   */
  public Pair<Map<Integer, List<LongRange>>, Map<String, List<LongRange>>> route(final List<LongRange> dataKeyRanges) {
    final Map<Integer, List<LongRange>> localKeyRangesMap = new HashMap<>();
    final Map<String, List<LongRange>> remoteKeyRangesMap = new HashMap<>();

    // perform routing for each dataKeyRanges of the operation
    for (final LongRange dataKeyRange : dataKeyRanges) {
      final Pair<Map<Integer, List<LongRange>>, Map<String, List<LongRange>>> routingResult = route(dataKeyRange);

      final Map<Integer, List<LongRange>> partialLocalKeyRangesMap = routingResult.getFirst();
      // merge local sub ranges that targets same block
      for (final Map.Entry<Integer, List<LongRange>> localEntry : partialLocalKeyRangesMap.entrySet()) {
        final List<LongRange> localRanges = localKeyRangesMap.get(localEntry.getKey());
        if (localRanges != null) {
          localRanges.addAll(localEntry.getValue());
        } else {
          localKeyRangesMap.put(localEntry.getKey(), localEntry.getValue());
        }
      }

      final Map<String, List<LongRange>> partialRemoteKeyRangesMap = routingResult.getSecond();
      // merge sub ranges held by the same remote evaluator
      for (final Map.Entry<String, List<LongRange>> remoteEntry : partialRemoteKeyRangesMap.entrySet()) {
        final List<LongRange> remoteRanges = remoteKeyRangesMap.get(remoteEntry.getKey());
        if (remoteRanges != null) {
          remoteRanges.addAll(remoteEntry.getValue());
        } else {
          remoteKeyRangesMap.put(remoteEntry.getKey(), remoteEntry.getValue());
        }
      }
    }
    return new Pair<>(localKeyRangesMap, remoteKeyRangesMap);
  }

  /**
   * Decompose a key range into local ranges and remote ranges.
   * @param dataKeyRange a key range
   * @return a map composed of a block id and a corresponding key range list and
   * a map composed of an endpoint id of remote evaluator and a corresponding key range list.
   */
  public Pair<Map<Integer, List<LongRange>>, Map<String, List<LongRange>>> route(final LongRange dataKeyRange) {
    final Map<Integer, List<LongRange>> localKeyRanges = new HashMap<>();
    final Map<String, List<LongRange>> remoteKeyRanges = new HashMap<>();

    final Map<Integer, SortedSet<Long>> blockToKeysMap = new HashMap<>();

    for (long dataKey = dataKeyRange.getMinimumLong(); dataKey <= dataKeyRange.getMaximumLong(); dataKey++) {
      final int blockId = blockResolver.getBlockId(dataKey);
      if (!blockToKeysMap.containsKey(blockId)) {
        blockToKeysMap.put(blockId, new TreeSet<Long>());
      }
      final SortedSet<Long> dataKeys = blockToKeysMap.get(blockId);
      dataKeys.add(dataKey);
    }

    // translate ids to ranges
    for (final Map.Entry<Integer, SortedSet<Long>> blockToKeys : blockToKeysMap.entrySet()) {
      final List<LongRange> rangeList =
          new ArrayList<>(LongRangeUtils.generateDenseLongRanges(blockToKeys.getValue()));
      final int blockId = blockToKeys.getKey();
      final int memoryStoreId = blockIdToStoreId[blockId];
      if (memoryStoreId == localStoreId) {
        localKeyRanges.put(blockId, rangeList);
      } else {
        remoteKeyRanges.put(getEvalId(memoryStoreId), rangeList);
      }
    }

    return new Pair<>(localKeyRanges, remoteKeyRanges);
  }

  /**
   * Returns the routing result for the given {@code dataId}.
   * It returns the endpoint id of the evaluator that owns the data whose id is {@code dataId}.
   * A boolean value is piggybacked, which indicates whether the data is in the local memory store.
   * So the caller does not need to check that the target evaluator is local or not.
   *
   * @param dataId an id of data
   * @return a pair of an id of block and and an Optional with an endpoint id of a target evaluator
   */
  public Pair<Integer, Optional<String>> route(final long dataId) {
    final int blockId = blockResolver.getBlockId(dataId);
    final int memoryStoreId = blockIdToStoreId[blockId];
    if (memoryStoreId == localStoreId) {
      return new Pair<>(blockId, Optional.<String>empty());
    } else {
      return new Pair<>(blockId, Optional.of(getEvalId(memoryStoreId)));
    }
  }

  public List<Integer> getLocalBlockIds() {
    return Collections.unmodifiableList(localBlocks);
  }

  /**
   * Converts the MemoryStore id to the corresponding Evaluator's endpoint id.
   * MemoryStore id is assumed to be assigned by the suffix of context id
   * (See {@link edu.snu.cay.services.em.driver.impl.PartitionManager#registerEvaluator(String, int)})
   * @param memoryStoreId MemoryStore's identifier
   * @return the endpoint id to access the MemoryStore.
   */
  private String getEvalId(final int memoryStoreId) {
    return evalPrefix + '-' + memoryStoreId;
  }
}
