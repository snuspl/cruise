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

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;

import java.util.*;

/**
 * A manager class for tracking tablets, which consist of multiple blocks, across associated executors.
 * Note that each table has own TabletManager instance.
 *
 * At initialization, associated executors are assigned with the evenly partitioned number of blocks.
 * After then, it manages addition and deletion of tablets and change in block distribution between tablets.
 * TODO #12: implement TabletManager
 */
@Private
@DriverSide
final class TabletManager {

  /**
   * One TabletManager is instantiated for each Table when a table is allocated to executors.
   * @param numTotalBlocks the number of total blocks
   * @param associatedExecutorIdSet the set of ids of initially associated executors
   */
  TabletManager(final int numTotalBlocks, final Set<String> associatedExecutorIdSet) {

  }

  /**
   * Allocates a new tablet to a newly associated executor.
   * @param executorId id of Executor
   */
  synchronized boolean addTablet(final String executorId) {
    return true;
  }

  /**
   * Deallocates a partition of a Executor, which was an associated executor.
   * @param executorId id of Executor
   */
  synchronized boolean deleteTablet(final String executorId) {
    return true;
  }

  /**
   * @return a list of ids of associated executors
   */
  synchronized List<String> getAssociatedExecutorIds() {
    return Collections.emptyList();
  }

  /**
   * @return a map contains a set of allocated blocks per associated executors
   */
  synchronized Map<String, Set<Long>> getExecutorIdToBlockIdSet() {
    return Collections.emptyMap();
  }

  /**
   * Returns the current locations of Blocks.
   * @return a list of executor ids, whose index is an block id.
   */
  synchronized List<String> getBlockLocations() {
    return Collections.emptyList();
  }

  /**
   * Chooses the Blocks in the Evaluator to move to another Evaluator.
   * The chosen Blocks cannot be chosen for another move until released by {@link #releaseBlockFromMove(int)}.
   * @param executorId id of Executor to choose the Blocks
   * @param numBlocks the maximum number of Blocks to choose
   * @return list of block ids that have been chosen.
   */
  synchronized List<Integer> chooseBlocksToMove(final String executorId, final int numBlocks) {
    return Collections.emptyList();
  }

  /**
   * Updates the owner of the Block to another Executor.
   * The block should be locked by {@link #chooseBlocksToMove(String, int)} to change its owner.
   * @param blockId id of the block to update its owner
   * @param oldOwnerId id of the MemoryStore who used to own the block
   * @param newOwnerId id of the MemoryStore who will own the block
   */
  synchronized void updateOwner(final int blockId, final String oldOwnerId, final String newOwnerId) {

  }

  /**
   * Releases the block that was locked by {@link #chooseBlocksToMove(String, int)}.
   * After then the block can be chosen for move again.
   * @param blockId id of the block
   */
  synchronized void releaseBlockFromMove(final int blockId) {

  }
}
