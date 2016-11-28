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
 * A manager class for tracking table partitions, which consist of multiple blocks, across associated containers.
 * Note that each table has own PartitionManager instance.
 *
 * At initialization, associated containers are assigned with the evenly partitioned number of blocks.
 * After then, it manages addition and deletion of partitions and change in block distribution between partitions.
 * TODO #12: implement PartitionManager
 */
@Private
@DriverSide
final class PartitionManager {

  /**
   * One PartitionManager is instantiated for each Table when a table is allocated to containers.
   * @param numTotalBlocks the number of total blocks
   * @param associatedContainerIdSet the set of ids of initially associated containers
   */
  PartitionManager(final int numTotalBlocks, final Set<String> associatedContainerIdSet) {

  }

  /**
   * Allocates a new partition to a new associator.
   * @param containerId id of Container
   */
  synchronized boolean addPartition(final String containerId) {
    return true;
  }

  /**
   * Deallocates a partition of a Container, which was an associator.
   * @param containerId id of Container
   */
  synchronized boolean deletePartition(final String containerId) {
    return true;
  }

  /**
   * @return a list of ids of associated containers
   */
  synchronized List<String> getAssociatedContainerIds() {
    return Collections.emptyList();
  }

  /**
   * @return a map contains a set of allocated blocks per associated containers
   */
  synchronized Map<String, Set<Long>> getContainerIdToBlockIdSet() {
    return Collections.emptyMap();
  }

  /**
   * Returns the current locations of Blocks.
   * @return a list of container ids, whose index is an block id.
   */
  synchronized List<String> getBlockLocations() {
    return Collections.emptyList();
  }

  /**
   * Chooses the Blocks in the Evaluator to move to another Evaluator.
   * The chosen Blocks cannot be chosen for another move until released by {@link #releaseBlockFromMove(int)}.
   * @param containerId id of Container to choose the Blocks
   * @param numBlocks the maximum number of Blocks to choose
   * @return list of block ids that have been chosen.
   */
  synchronized List<Integer> chooseBlocksToMove(final String containerId, final int numBlocks) {
    return Collections.emptyList();
  }

  /**
   * Updates the owner of the Block to another Container.
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
