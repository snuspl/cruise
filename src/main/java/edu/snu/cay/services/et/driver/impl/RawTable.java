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
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.exceptions.NotAssociatedTableException;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Represents a Table, which only exists in the master-side logically.
 *
 * Containers can subscribe and associate with the table, but there is no effect until the table is materialized.
 * {@link #materialize()} partitions the table and physically allocates the partitions to associated executors.
 *
 *  Note that if any executor has not associated with the table yet, it fails with {@link NotAssociatedTableException}
 *  because no one can take its partition.
 */
public final class RawTable {
  private final TableConfiguration tableConf;
  private final TableManager tableManager;
  private final MigrationManager migrationManager;
  private final TableInitializer tableInitializer;

  private final Set<String> associatedContainerIds = ConcurrentHashMap.newKeySet();
  private final Set<String> subscribedContainerIds = ConcurrentHashMap.newKeySet();

  RawTable(final TableConfiguration tableConf,
           final TableManager tableManager,
           final MigrationManager migrationManager,
           final TableInitializer tableInitializer) {
    this.tableConf = tableConf;
    this.tableManager = tableManager;
    this.migrationManager = migrationManager;
    this.tableInitializer = tableInitializer;
  }

  /**
   * Subscribe the table. The Containers will receive the updates in ownership information for this table.
   * Note that the Containers do not receive the updates until {@link #materialize()} is called.
   * @param executors a list of executors
   * @return this
   */
  public synchronized RawTable subscribe(final List<AllocatedExecutor> executors) {
    subscribedContainerIds.addAll(executors.stream()
        .map(AllocatedExecutor::getId).collect(Collectors.toList()));
    return this;
  }

  /**
   * Associate with the table. The Containers will take some portion of this table into its partition.
   * Note that the Containers do not receive the partition until {@link #materialize()} is called.
   * @param executors a list of executors
   * @return this
   */
  public synchronized RawTable associate(final List<AllocatedExecutor> executors) {
    associatedContainerIds.addAll(executors.stream()
        .map(AllocatedExecutor::getId).collect(Collectors.toList()));
    return this;
  }


  /**
   * Materializes the table to associated executors and also initializes table in subscribers.
   * @return an {@link MaterializedTable}
   * @throws NotAssociatedTableException when no executor has associated with the table.
   */
  public synchronized MaterializedTable materialize() throws NotAssociatedTableException {
    if (associatedContainerIds.isEmpty()) {
      throw new NotAssociatedTableException();
    }

    // partition table into associators
    final PartitionManager partitionManager =
        new PartitionManager(tableConf.getNumTotalBlocks(), associatedContainerIds);
    // register subscribers to MigrationManager, which will broadcast ownership update
    for (final String executorId : subscribedContainerIds) {
      migrationManager.registerSubscription(tableConf.getId(), executorId);
    }

    // initialize table partitions in associators and subscribers
    tableInitializer.initTableInAssociators(tableConf, associatedContainerIds,
        partitionManager.getExecutorIdToBlockIdSet());
    tableInitializer.initTableInSubscribers(tableConf, subscribedContainerIds,
        partitionManager.getBlockLocations());

    final MaterializedTable materializedTable =
        new MaterializedTable(tableConf, partitionManager, migrationManager, tableInitializer);

    // register MaterializedTable to TableManager
    tableManager.onMaterializedTable(materializedTable);
    return materializedTable;
  }
}
