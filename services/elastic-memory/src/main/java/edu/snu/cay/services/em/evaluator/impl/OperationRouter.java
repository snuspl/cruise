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
   * so prefix should be added to get the Evaluator's endpoint id.
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
    initRouter();
  }

  /**
   * Initializes the routing table and its local block list.
   */
  private void initRouter() {
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
   * Initializes the router, providing a prefix of evaluator to locate remote evaluators.
   * It is invoked when the service context is started.
   */
  public void initialize(final String endPointId) {
    this.evalPrefix = endPointId.split("-")[0];
    LOG.log(Level.INFO, "Initialize router with localEndPointId: {0}", endPointId);
  }

  /**
   * Routes the data key range of the operation.
   * @param dataKeyRanges a range of data keys
   * @return a pair of a map between a block id and a corresponding sub key range,
   * and a map between evaluator id and corresponding sub key ranges.
   */
  public Pair<Map<Integer, LongRange>, Map<String, List<LongRange>>> route(final List<LongRange> dataKeyRanges) {
    final Map<Integer, LongRange> localBlockToSubKeyRangeMap = new HashMap<>();
    final Map<String, List<LongRange>> remoteEvalToSubKeyRangesMap = new HashMap<>();

    // dataKeyRanges has at least one element
    // In most cases, there are only one range in dataKeyRanges
    for (final LongRange keyRange : dataKeyRanges) {

      final Map<Integer, LongRange> blockToSubKeyRangeMap = resolveBlocks(keyRange);
      for (final Map.Entry<Integer, LongRange> blockToSubKeyRange : blockToSubKeyRangeMap.entrySet()) {
        final int blockId = blockToSubKeyRange.getKey();
        final LongRange subKeyRange = blockToSubKeyRange.getValue();

        final Optional<String> remoteEvalId = resolveEval(blockId);

        // aggregate sub ranges for the same evaluator
        if (remoteEvalId.isPresent()) {
          if (!remoteEvalToSubKeyRangesMap.containsKey(remoteEvalId.get())) {
            remoteEvalToSubKeyRangesMap.put(remoteEvalId.get(), new LinkedList<LongRange>());
          }
          final List<LongRange> remoteRangeList = remoteEvalToSubKeyRangesMap.get(remoteEvalId.get());
          remoteRangeList.add(subKeyRange);
        } else {
          localBlockToSubKeyRangeMap.put(blockId, subKeyRange);
        }
      }
    }

    return new Pair<>(localBlockToSubKeyRangeMap, remoteEvalToSubKeyRangesMap);
  }

  /**
   * Resolves a block id for a data key.
   * @param dataKey a key of data
   * @return a block id
   */
  public int resolveBlock(final Long dataKey) {
    return blockResolver.resolveBlock(dataKey);
  }

  /**
   * Resolves block ids for a range of data keys.
   * Each block contains a sub key range.
   * @param dataKeyRange a range of data keys
   * @return a map between a block id and a range of data keys
   */
  public Map<Integer, LongRange> resolveBlocks(final LongRange dataKeyRange) {
    return blockResolver.resolveBlocks(dataKeyRange);
  }

  /**
   * Resolves an evaluator id for a block id.
   * It returns empty when the block belongs to the local MemoryStore.
   * @param blockId an id of block
   * @return an Optional with an evaluator id
   */
  public Optional<String> resolveEval(final int blockId) {
    final int memoryStoreId = blockIdToStoreId[blockId];
    if (memoryStoreId == localStoreId) {
      return Optional.empty();
    } else {
      return Optional.of(getEvalId(memoryStoreId));
    }
  }

  /**
   * @return a list of block ids in the local MemoryStore
   */
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
