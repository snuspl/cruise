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

import javax.inject.Inject;

/**
 * A manager class administrates migration of blocks between containers.
 * Note that tables share one instance of {@link MigrationManager}.
 * TODO #13: implement migration manager
 */
@Private
@DriverSide
final class MigrationManager {

  @Inject
  private MigrationManager() {
  }

  /**
   * Registers subscribers for the update of partition status of a table whose id is {@code tableId}.
   * Whenever a block has been moved, the container with {@code containerId} will be notified.
   * @param tableId a table id
   * @param containerId a container id
   */
  void registerSubscription(final String tableId, final String containerId) {

  }

  /**
   * Moves the {@code numBlocks} number of blocks of {@code partitionManager} from src container to dst container.
   * @param partitionManager a {@link PartitionManager} of a table to be moved
   * @param srcContainerId an id of src container
   * @param dstContainerId an id of dst container
   * @param numBlocks the number of blocks to move
   */
  void moveBlocks(final PartitionManager partitionManager,
                  final String srcContainerId, final String dstContainerId, final int numBlocks) {
    // use following methods to update driver-side block partitioning
    // partitionManager.chooseBlocksToMove()
    // partitionManager.updateOwner()
    // partitionManager.releaseBlockFromMove()
  }

  // other methods would be added more
}
