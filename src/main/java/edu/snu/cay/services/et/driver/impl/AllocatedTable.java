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

import javax.inject.Inject;
import java.util.List;

/**
 * Represents a state where the table is completely allocated into executors.
 * Now the executors that associate this table have the actual reference of it, and the executors subscribing it
 * receive the ownership information whenever there is any change.
 *
 * Even the table is distributed to executors already, more executors are allowed to subscribe or associate with it,
 * then the changes will be applied to executors immediately.
 */
public final class AllocatedTable {
  private final TabletManager tabletManager;
  private final MigrationManager migrationManager;
  private final TableInitializer tableInitializer;

  private TableConfiguration tableConf;

  @Inject
  private AllocatedTable(final TabletManager tabletManager,
                         final MigrationManager migrationManager,
                         final TableInitializer tableInitializer) {
    this.tabletManager = tabletManager;
    this.migrationManager = migrationManager;
    this.tableInitializer = tableInitializer;
  }

  /**
   * Initializes {@link this} by allocating a table
   * with a given {@code tableConfiguration} to {@code initialAssociators}.
   * This method should be called once before other methods.
   * @param tableConfiguration a table configuration
   * @param initialAssociators a list of initial executors to be associated to a table
   */
  synchronized void init(final TableConfiguration tableConfiguration,
                         final List<AllocatedExecutor> initialAssociators) {
    tableConf = tableConfiguration;

    // partition table into initialAssociators
    tabletManager.init(initialAssociators);

    // initialize tablets in initialAssociators
    tableInitializer.initTable(tableConfiguration, initialAssociators, tabletManager.getOwnershipStatus(), true);
  }

  /**
   * Subscribes the table. The executors will receive the updates in ownership information for this table.
   * @param executors a list of executors
   */
  public synchronized void subscribe(final List<AllocatedExecutor> executors) {
    for (final AllocatedExecutor executor : executors) {
      migrationManager.registerSubscription(tableConf.getId(), executor.getId());
    }
    tableInitializer.initTable(tableConf, executors, tabletManager.getOwnershipStatus(), false);
  }

  /**
   * Associates with the table. The executors will take some portion of this table into its tablet.
   * @param executors a list of executors
   */
  public synchronized void associate(final List<AllocatedExecutor> executors) {
    for (final AllocatedExecutor executor : executors) {
      tabletManager.addTablet(executor.getId());
    }

    tableInitializer.initTable(tableConf, executors, tabletManager.getOwnershipStatus(), true);
  }

  /**
   * Moves the {@code numBlocks} number of blocks from src executor to dst executor.
   * @param srcExecutorId an id of src executor
   * @param dstExecutorId an id of dst executor
   * @param numBlocks the number of blocks to move
   */
  public synchronized void moveBlocks(final String srcExecutorId,
                                      final String dstExecutorId,
                                      final int numBlocks) {
    migrationManager.moveBlocks(tabletManager, srcExecutorId, dstExecutorId, numBlocks);
  }

  /**
   * @return a configuration of the table
   */
  TableConfiguration getTableConfiguration() {
    return tableConf;
  }

  /**
   * @return a list of executors associated with the table
   */
  List<String> getAssociatedExecutorIds() {
    return tabletManager.getAssociatedExecutorIds();
  }
}
