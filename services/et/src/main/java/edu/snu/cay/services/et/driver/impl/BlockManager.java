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
package edu.snu.cay.services.et.driver.impl;

import edu.snu.cay.services.et.configuration.parameters.NumTotalBlocks;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A manager class for tracking blocks, across associated executors.
 * Note that each table has own BlockManager instance.
 *
 * At initialization, associated executors are assigned with the evenly partitioned number of blocks.
 * After then, it manages registration of executors and change in block distribution between executors.
 */
@Private
@DriverSide
@ThreadSafe
public final class BlockManager {
  private static final Logger LOG = Logger.getLogger(BlockManager.class.getName());

  /**
   * A mapping that maintains which block is owned by which Executor.
   */
  private final Map<Integer, String> blockIdToExecutorId;

  /**
   * A mapping that maintains each Executor has taken which blocks.
   */
  private final Map<String, Set<Integer>> executorIdToBlockIds;

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
    this.blockIdToExecutorId = new ConcurrentHashMap<>(numTotalBlocks);
    this.executorIdToBlockIds = new ConcurrentHashMap<>();
    this.movingBlocks = new HashSet<>(numTotalBlocks);
    this.numTotalBlocks = numTotalBlocks;
  }

  /**
   * Initialize {@link this} by partitioning blocks evenly to {@code executors}.
   * This method should be called once before other methods.
   * @param executorIds a set of executor ids that the table is initially assigned to
   */
  public void init(final Set<String> executorIds) {
    final List<String> executorIdList = new ArrayList<>(executorIds);

    for (int blockId = 0; blockId < numTotalBlocks; blockId++) {
      final String executorId = executorIdList.get(blockId % executorIdList.size());
      blockIdToExecutorId.put(blockId, executorId);

      if (!executorIdToBlockIds.containsKey(executorId)) {
        executorIdToBlockIds.put(executorId, Collections.newSetFromMap(new ConcurrentHashMap<>()));
      }
      executorIdToBlockIds.get(executorId).add(blockId);
    }
  }

  /**
   * Registers a new associated executor.
   * @param executorId id of Executor
   */
  synchronized void registerExecutor(final String executorId) {
    if (executorIdToBlockIds.containsKey(executorId)) {
      throw new RuntimeException(String.format("Executor %s is already registered", executorId));
    }

    executorIdToBlockIds.put(executorId, Collections.newSetFromMap(new ConcurrentHashMap<>()));
  }

  /**
   * Deregisters an executor.
   * @param executorId id of Executor
   */
  synchronized void deregisterExecutor(final String executorId) {
    if (!executorIdToBlockIds.containsKey(executorId)) {
      throw new RuntimeException(String.format("Executor %s is not registered", executorId));
    }
    if (!executorIdToBlockIds.get(executorId).isEmpty()) {
      throw new RuntimeException("This attempt tries to deregister an executor with remaining blocks," +
          " resulting missing blocks.");
    }

    executorIdToBlockIds.remove(executorId);
  }

  /**
   * @return a set of associator ids
   */
  synchronized Set<String> getAssociatorIds() {
    return new HashSet<>(executorIdToBlockIds.keySet());
  }

  /**
   * Returns a partition information.
   * @return a map between an executor id and a set of block ids
   */
  synchronized Map<String, Set<Integer>> getPartitionInfo() {
    return new HashMap<>(executorIdToBlockIds);
  }

  /**
   * @param blockId id of the block to resolve its owner
   * @return the owner's id of the block
   */
  synchronized String getOwnerId(final int blockId) {
    return blockIdToExecutorId.get(blockId);
  }

  /**
   * Returns the current locations of Blocks.
   * @return a list of executor ids, whose index is an block id.
   */
  public synchronized List<String> getOwnershipStatus() {
    return new ArrayList<>(new TreeMap<>(blockIdToExecutorId).values());
  }

  /**
   * Chooses the Blocks in the Evaluator to move to another Evaluator.
   * The chosen Blocks cannot be chosen for another move until released by {@link #releaseBlockFromMove(int)}.
   * @param executorId id of Executor to choose the Blocks
   * @param numBlocks the maximum number of Blocks to choose
   * @return list of block ids that have been chosen.
   */
  synchronized List<Integer> chooseBlocksToMove(final String executorId, final int numBlocks) {
    if (numBlocks <= 0) {
      return Collections.emptyList();
    }

    final Set<Integer> blockIds = executorIdToBlockIds.get(executorId);
    if (blockIds == null) {
      throw new RuntimeException(String.format("Executor %s is not registered", executorId));
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
      LOG.log(Level.WARNING, "{0} blocks are chosen from executor {1}," +
              " while {2} blocks are requested. Blocks: {3}",
          new Object[] {blockIdList.size(), executorId, numBlocks, blockIdList});
    } else {
      LOG.log(Level.FINEST, "{0} blocks are chosen from executor {1}. Blocks: {2}",
          new Object[] {numBlocks, executorId, blockIdList});
    }

    return blockIdList;
  }

  /**
   * Updates the owner of the Block to another Executor.
   * The block should be locked by {@link #chooseBlocksToMove(String, int)} to change its owner.
   * @param blockId id of the block to update its owner
   * @param oldOwnerId id of the MemoryStore who used to own the block
   * @param newOwnerId id of the MemoryStore who will own the block
   */
  synchronized void updateOwner(final int blockId, final String oldOwnerId, final String newOwnerId) {
    LOG.log(Level.FINER, "Update owner of block {0} from store {1} to store {2}",
        new Object[]{blockId, oldOwnerId, newOwnerId});

    if (blockId > numTotalBlocks || blockId < 0) {
      throw new RuntimeException(String.format("Invalid blockId: %d, NumTotalBlocks: %d", blockId, numTotalBlocks));
    }
    if (!movingBlocks.contains(blockId)) {
      throw new RuntimeException(String.format("The block %d has not been chosen for migration" +
          " or has already been released", blockId));
    }

    if (!executorIdToBlockIds.containsKey(oldOwnerId)) {
      throw new RuntimeException(String.format("Executor %s has been lost.", oldOwnerId));
    }
    if (!executorIdToBlockIds.containsKey(newOwnerId)) {
      throw new RuntimeException(String.format("Executor %s has been lost.", newOwnerId));
    }

    if (!executorIdToBlockIds.get(oldOwnerId).contains(blockId)) {
      throw new RuntimeException(String.format("Executor %s does not own block %d", oldOwnerId, blockId));
    }
    if (executorIdToBlockIds.get(newOwnerId).contains(blockId)) {
      throw new RuntimeException(String.format("Executor %s already owns block %d", newOwnerId, blockId));
    }

    final Set<Integer> blocksInOldExecutor = executorIdToBlockIds.get(oldOwnerId);
    final Set<Integer> blocksInNewExecutor = executorIdToBlockIds.get(newOwnerId);
    blocksInOldExecutor.remove(blockId);
    blocksInNewExecutor.add(blockId);

    blockIdToExecutorId.put(blockId, newOwnerId);
  }

  /**
   * Releases the block that was locked by {@link #chooseBlocksToMove(String, int)}.
   * After then the block can be chosen for move again.
   * @param blockId id of the block
   */
  synchronized void releaseBlockFromMove(final int blockId) {
    final boolean removed = movingBlocks.remove(blockId);
    if (!removed) {
      LOG.log(Level.WARNING, "The block {0} has already been marked as finished", blockId);
    }
  }

  /**
   * @param executorId id of the Executor
   * @return the number of blocks owned by the Executor.
   */
  synchronized int getNumBlocks(final String executorId) {
    final Set<Integer> blockIds = executorIdToBlockIds.get(executorId);
    // the given eval id is not registered
    if (blockIds == null) {
      return 0;
    }
    return blockIds.size();
  }
}
