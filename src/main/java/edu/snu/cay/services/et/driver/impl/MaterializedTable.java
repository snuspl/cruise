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

import edu.snu.cay.services.et.configuration.TableConfiguration;
import edu.snu.cay.services.et.driver.api.AllocatedContainer;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Represents a state where the table is created both logically and physically.
 * Now the Containers that associate this table have the actual reference of it, and the Containers subscribing it
 * receive the ownership information whenever there is any change.
 *
 * Even the table is distributed to Containers already, more containers are allowed to subscribe or associate with it,
 * then the changes will be applied to Containers immediately.
 */
public final class MaterializedTable {
  private final TableConfiguration tableConf;
  private final PartitionManager partitionManager;
  private final MigrationManager migrationManager;
  private final TableInitializer tableInitializer;

  MaterializedTable(final TableConfiguration tableConf,
                    final PartitionManager partitionManager,
                    final MigrationManager migrationManager,
                    final TableInitializer tableInitializer) {
    this.tableConf = tableConf;
    this.partitionManager = partitionManager;
    this.migrationManager = migrationManager;
    this.tableInitializer = tableInitializer;
  }

  /**
   * Subscribes the table. The Containers will receive the updates in ownership information for this table.
   * @param containers a list of containers
   * @return this
   */
  public synchronized MaterializedTable subscribe(final List<AllocatedContainer> containers) {
    final Set<String> containerIdSet = new HashSet<>();
    for (final AllocatedContainer container : containers) {
      migrationManager.registerSubscription(tableConf.getId(), container.getId());
      containerIdSet.add(container.getId());
    }
    tableInitializer.initTableInSubscribers(tableConf, containerIdSet,
        partitionManager.getBlockLocations());
    return this;
  }

  /**
   * Associates with the table. The Containers will take some portion of this table into its partition.
   * @param containers a list of containers
   * @return this
   */
  public synchronized MaterializedTable associate(final List<AllocatedContainer> containers) {
    final Set<String> containerIdSet = new HashSet<>();
    for (final AllocatedContainer container : containers) {
      partitionManager.addPartition(container.getId());
    }
    tableInitializer.initTableInAssociators(tableConf, containerIdSet,
        partitionManager.getContainerIdToBlockIdSet());
    return this;
  }

  /**
   * Moves the {@code numBlocks} number of blocks from src container to dst container.
   * @param srcContainerId an id of src container
   * @param dstContainerId an id of dst container
   * @param numBlocks the number of blocks to move
   */
  public synchronized void moveBlocks(final String srcContainerId,
                                      final String dstContainerId,
                                      final int numBlocks) {
    migrationManager.moveBlocks(partitionManager, srcContainerId, dstContainerId, numBlocks);
  }

  /**
   * @return a configuration of the table
   */
  TableConfiguration getTableConfiguration() {
    return tableConf;
  }

  /**
   * @return a list of containers associated with the table
   */
  List<String> getAssociatedContainerIds() {
    return partitionManager.getAssociatedContainerIds();
  }
}
