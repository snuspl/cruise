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
package edu.snu.cay.services.em.driver.impl;

import edu.snu.cay.services.em.common.parameters.NumTotalBlocks;
import edu.snu.cay.services.em.optimizer.api.EvaluatorParameters;
import edu.snu.cay.services.em.optimizer.impl.DataInfoImpl;
import edu.snu.cay.services.em.optimizer.impl.EvaluatorParametersImpl;
import net.jcip.annotations.ThreadSafe;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manager class for keeping track of blocks assigned to each store in evaluators.
 * At system initialization, evaluators are assigned with MemoryStores of evenly partitioned number of blocks.
 * After init, it maintains a global view of the blocks allocated to MemoryStores
 * and guarantees that any block never become duplicate or missing.
 * This class is thread-safe, since all method accessing shared states are synchronized.
 */
// TODO #90: We need to handle the failures for Move.
@ThreadSafe
@DriverSide
@Private
public final class BlockManager {
  private static final Logger LOG = Logger.getLogger(BlockManager.class.getName());

  private final AtomicInteger numEvalCounter = new AtomicInteger(0);

  /**
   * This is set when one of the Evaluator is registered.
   */
  private String evalIdPrefix = null;

  /**
   * A mapping that maintains each MemoryStore have taken which blocks.
   */
  private final Map<Integer, Set<Integer>> storeIdToBlockIds;

  /**
   * A mapping that maintains which block is owned by which MemoryStore.
   */
  private final Map<Integer, Integer> blockIdToStoreId;

  /**
   * Holds the block ids which are being moved.
   */
  private final Set<Integer> movingBlocks;

  /**
   * The number of total blocks.
   */
  private final int numTotalBlocks;

  @Inject
  private BlockManager(@Parameter(NumTotalBlocks.class) final int numTotalBlocks) {
    this.storeIdToBlockIds = new HashMap<>();
    this.blockIdToStoreId = new HashMap<>();
    this.movingBlocks = new HashSet<>(numTotalBlocks);
    this.numTotalBlocks = numTotalBlocks;
  }

  /**
   * Register an evaluator and allocate blocks to the evaluator.
   * MemoryStore's identifier is assigned as the suffix of the Context id;
   * {@link edu.snu.cay.services.em.evaluator.impl.OperationRouter} also routes requests based on this rule.
   *
   * @param contextId an id of context
   * @param numInitialEvals the number of initial evaluators
   * @return id of the allocated MemoryStore
   */
  public synchronized int registerEvaluator(final String contextId,
                                            final int numInitialEvals) {
    if (evalIdPrefix == null) {
      // TODO #509: remove the assumption on the format of contextId
      // The same prefix is used in ElasticMemoryConfiguration and OperationRouter.
      evalIdPrefix = contextId.split("-")[0];
    }

    final int memoryStoreId = getMemoryStoreId(contextId);
    if (storeIdToBlockIds.containsKey(memoryStoreId)) {
      throw new RuntimeException("This evaluator is already registered. Its context Id is " + contextId);
    }
    storeIdToBlockIds.put(memoryStoreId, new HashSet<Integer>());

    final int numActiveEvals = numEvalCounter.incrementAndGet();
    LOG.log(Level.FINE, "MemoryStore {0} is assigned to eval {1}", new Object[]{memoryStoreId, contextId});

    // Fixed number of Blocks are assigned to the initial MemoryStores. When MemoryStores are created
    // by EM.add(), they are empty at first; existing blocks are moved to the new MemoryStores by EM.move(),
    // which is the part of reconfiguration plan generated by Optimizer (EM doesn't redistribute data voluntarily).
    if (numActiveEvals <= numInitialEvals) {

      // If NumTotalBlocks = 31 and NumInitialEval = 5, then MemoryStore0 takes {0, 5, 10, 15, 20, 25, 30}
      // and MemoryStore3 takes {3, 8, 13, 18, 23, 28}
      for (int blockId = memoryStoreId; blockId < numTotalBlocks; blockId += numInitialEvals) {
        storeIdToBlockIds.get(memoryStoreId).add(blockId);
        blockIdToStoreId.put(blockId, memoryStoreId);
      }
    }
    return memoryStoreId;
  }

  /**
   * Deregister an evaluator.
   * It should be called after all blocks in the store of the evaluator are completely moved out.
   * @param contextId an id of context
   */
  public synchronized void deregisterEvaluator(final String contextId) {
    final int memoryStoreId = getMemoryStoreId(contextId);

    final Set<Integer> remainingBlocks = storeIdToBlockIds.remove(memoryStoreId);
    if (remainingBlocks == null) {
      throw new RuntimeException("The store " + memoryStoreId + " does not exist");
    } else if (!remainingBlocks.isEmpty()) {
      throw new RuntimeException("This attempt tries to remove a non-empty store, resulting missing blocks.");
    }
  }

  /**
   * Return the current locations of blocks.
   * @return a list of store ids, whose index is an block id.
   */
  synchronized List<Integer> getBlockLocations() {
    // sort the map with key and get its values
    return new ArrayList<>(new TreeMap<>(blockIdToStoreId).values());
  }

  /**
   * @return a list of active evaluators that own each MemoryStore.
   */
  synchronized Set<String> getActiveEvaluators() {
    final Set<Integer> activeStores = storeIdToBlockIds.keySet();
    final Set<String> activeEvaluators = new HashSet<>(activeStores.size());

    for (final int storeId : activeStores) {
      activeEvaluators.add(getEvaluatorId(storeId));
    }
    return activeEvaluators;
  }

  /**
   * Updates the owner of the block to another MemoryStore.
   * @param blockId id of the block to update its owner
   * @param oldOwnerId id of the MemoryStore who used to own the block
   * @param newOwnerId id of the MemoryStore who will own the block
   */
  synchronized void updateOwner(final int blockId, final int oldOwnerId, final int newOwnerId) {
    LOG.log(Level.FINER, "Update owner of block {0} from store {1} to store {2}",
        new Object[]{blockId, oldOwnerId, newOwnerId});

    if (!storeIdToBlockIds.containsKey(oldOwnerId)) {
      throw new RuntimeException("MemoryStore " + oldOwnerId + " has been lost.");
    }

    if (!storeIdToBlockIds.containsKey(newOwnerId)) {
      throw new RuntimeException("MemoryStore " + newOwnerId + " has been lost.");
    }

    if (!storeIdToBlockIds.get(oldOwnerId).remove(blockId)) {
      throw new RuntimeException("Store " + oldOwnerId + " does not own block " + blockId);
    }
    if (!storeIdToBlockIds.get(newOwnerId).add(blockId)) {
      throw new RuntimeException("Store " + newOwnerId + " already owns block " + blockId);
    }

    blockIdToStoreId.put(blockId, newOwnerId);
  }

  /**
   * @return the Driver's view of up-to-date mapping between MemoryStores and blocks.
   */
  Map<Integer, Set<Integer>> getStoreIdToBlockIds() {
    return Collections.unmodifiableMap(storeIdToBlockIds);
  }

  /**
   * @return the number of total blocks that exist in this Elastic Memory instance.
   */
  int getNumTotalBlocks() {
    return numTotalBlocks;
  }

  /**
   * Converts the Evaluator id to the MemoryStore id.
   */
  int getMemoryStoreId(final String evalId) {
    return Integer.valueOf(evalId.split("-")[1]);
  }

  /**
   * Converts the MemoryStore id to the Evaluator id.
   */
  private String getEvaluatorId(final int memoryStoreId) {
    return evalIdPrefix + "-" + memoryStoreId;
  }

  /**
   * Choose the blocks in the Evaluator to move to another Evaluator.
   * @param evalId id of Evaluator to choose the blocks
   * @param numBlocks the maximum number of blocks to choose
   * @return list of block ids that have been chosen.
   */
  synchronized List<Integer> chooseBlocksToMove(final String evalId, final int numBlocks) {
    final int storeId = getMemoryStoreId(evalId);
    final Set<Integer> blockIds = storeIdToBlockIds.get(storeId);

    if (blockIds == null) {
      throw new RuntimeException("Evaluator " + evalId + " is not registered");
    }

    final List<Integer> blockIdList = new ArrayList<>(Math.min(blockIds.size(), numBlocks));
    int count = 0;

    // Choose blocks at most the number of requested blocks. The blocks that are already moving, are skipped.
    for (final Integer blockId : blockIds) {
      if (!movingBlocks.contains(blockId)) {
        blockIdList.add(blockId);
        movingBlocks.add(blockId);
        count++;
      }

      if (count == numBlocks) {
        break;
      }
    }

    if (blockIdList.size() < numBlocks) {
      LOG.log(Level.WARNING, "{0} blocks are chosen from store {1} in evaluator {2}," +
          " while {3} blocks are requested. Blocks: {4}",
          new Object[] {blockIdList.size(), storeId, evalId, numBlocks, blockIdList});
    } else {
      LOG.log(Level.FINEST, "{0} blocks are chosen from store {1} in evaluator {2}. Blocks: {3}",
          new Object[] {numBlocks, storeId, evalId, blockIdList});
    }

    return blockIdList;
  }

  /**
   * @param evalId id of the Evaluator
   * @return the number of blocks owned by the Evaluator.
   */
  public synchronized int getNumBlocks(final String evalId) {
    final Set<Integer> blockIds = storeIdToBlockIds.get(getMemoryStoreId(evalId));
    // the given eval id is not registered
    if (blockIds == null) {
      return 0;
    }
    return blockIds.size();
  }

  /**
   * Mark the block as moved.
   * @param blockId id of the block
   */
  synchronized void markBlockAsMoved(final int blockId) {
    final boolean removed = movingBlocks.remove(blockId);
    if (!removed) {
      LOG.log(Level.WARNING, "The block {0} has already been marked as finished", blockId);
    }
  }

  private Map<String, Integer> getEvalIdToNumBlocks() {
    final Map<String, Integer> evalIdToNumBlocks = new HashMap<>();
    for (final Map.Entry<Integer, Set<Integer>> storeIdToblockId : storeIdToBlockIds.entrySet()) {
      final String evalId = getEvaluatorId(storeIdToblockId.getKey());
      evalIdToNumBlocks.put(evalId, storeIdToblockId.getValue().size());
    }
    return evalIdToNumBlocks;
  }

  Map<String, EvaluatorParameters> generateEvalParams() {
    final Map<String, Integer> evalIdToNumBlocks = getEvalIdToNumBlocks();
    final int numEvaluators = evalIdToNumBlocks.size();

    final Map<String, EvaluatorParameters> evaluatorsMap = new HashMap<>(numEvaluators);
    for (final Map.Entry<String, Integer> evalIdToNumBlock : evalIdToNumBlocks.entrySet()) {
      final String evalId = evalIdToNumBlock.getKey();
      final int numBlocks = evalIdToNumBlock.getValue();

      evaluatorsMap.put(evalId, new EvaluatorParametersImpl(evalId, new DataInfoImpl(numBlocks), new HashMap<>(0)));
    }
    return evaluatorsMap;
  }
}
