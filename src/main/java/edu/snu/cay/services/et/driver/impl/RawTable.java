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
 * Executors can subscribe and associate with the table, but there is no effect until the table is allocated.
 * {@link #allocate()} partitions the table and physically allocates the tablets to associated executors.
 *
 *  Note that if any executor has not associated with the table yet, it fails with {@link NotAssociatedTableException}
 *  because no one can take its tablet.
 */
public final class RawTable {
  private final TableConfiguration tableConf;
  private final TableManager tableManager;
  private final MigrationManager migrationManager;
  private final TableInitializer tableInitializer;

  private final Set<String> associatedExecutorIds = ConcurrentHashMap.newKeySet();
  private final Set<String> subscribedExecutorIds = ConcurrentHashMap.newKeySet();

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
   * Subscribe the table. The executors will receive the updates in ownership information for this table.
   * Note that the executors do not receive the updates until {@link #allocate()} is called.
   * @param executors a list of executors
   * @return this
   */
  public synchronized RawTable subscribe(final List<AllocatedExecutor> executors) {
    subscribedExecutorIds.addAll(executors.stream()
        .map(AllocatedExecutor::getId).collect(Collectors.toList()));
    return this;
  }

  /**
   * Associate with the table. The executors will take some portion of this table into its tablet.
   * Note that the executors do not receive the tablet until {@link #allocate()} is called.
   * @param executors a list of executors
   * @return this
   */
  public synchronized RawTable associate(final List<AllocatedExecutor> executors) {
    associatedExecutorIds.addAll(executors.stream()
        .map(AllocatedExecutor::getId).collect(Collectors.toList()));
    return this;
  }


  /**
   * Allocate the table to associated executors and also initializes table in subscribers.
   * @return an {@link AllocatedTable}
   * @throws NotAssociatedTableException when no executor has associated with the table.
   */
  public synchronized AllocatedTable allocate() throws NotAssociatedTableException {
    if (associatedExecutorIds.isEmpty()) {
      throw new NotAssociatedTableException();
    }

    // partition table into associators
    final TabletManager tabletManager =
        new TabletManager(tableConf.getNumTotalBlocks(), associatedExecutorIds);
    // register subscribers to MigrationManager, which will broadcast ownership update
    for (final String executorId : subscribedExecutorIds) {
      migrationManager.registerSubscription(tableConf.getId(), executorId);
    }

    // initialize tablets in associators and subscribers
    tableInitializer.initTableInAssociators(tableConf, associatedExecutorIds,
        tabletManager.getExecutorIdToBlockIdSet());
    tableInitializer.initTableInSubscribers(tableConf, subscribedExecutorIds,
        tabletManager.getBlockLocations());

    final AllocatedTable allocatedTable =
        new AllocatedTable(tableConf, tabletManager, migrationManager, tableInitializer);

    // register AllocatedTable to TableManager
    tableManager.onAllocatedTable(allocatedTable);
    return allocatedTable;
  }
}
