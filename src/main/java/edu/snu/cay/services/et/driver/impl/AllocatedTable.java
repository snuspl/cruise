/*
 * Copyright (C) 2017 Seoul National University
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
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.wake.EventHandler;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Represents a state where the table is completely allocated into executors.
 * Now the executors that associate this table have the actual reference of it, and the executors subscribing it
 * receive the ownership information whenever there is any change.
 *
 * Even the table is distributed to executors already, more executors are allowed to subscribe or associate with it,
 * then the changes will be applied to executors immediately.
 */
@DriverSide
public final class AllocatedTable {
  private static final Logger LOG = Logger.getLogger(AllocatedTable.class.getName());

  private final BlockManager blockManager;
  private final MigrationManager migrationManager;
  private final TableInitializer tableInitializer;

  private TableConfiguration tableConf;

  @Inject
  private AllocatedTable(final BlockManager blockManager,
                         final MigrationManager migrationManager,
                         final TableInitializer tableInitializer) {
    this.blockManager = blockManager;
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

    final Set<String> executorIds = new HashSet<>(initialAssociators.size());
    initialAssociators.forEach(executor -> {
      executorIds.add(executor.getId());
      migrationManager.registerSubscription(tableConf.getId(), executor.getId());
    });

    // partition table into blocks and initialize them in associators
    blockManager.init(executorIds);
    tableInitializer.initTable(tableConfiguration, executorIds, blockManager.getOwnershipStatus(), true);
  }

  /**
   * Subscribes the table. The executors will receive the updates in ownership information for this table.
   * @param executors a list of executors
   */
  public synchronized void subscribe(final List<AllocatedExecutor> executors) {
    final Set<String> executorIdSet = new HashSet<>(executors.size());
    executors.forEach(executor -> {
      migrationManager.registerSubscription(tableConf.getId(), executor.getId());
      executorIdSet.add(executor.getId());
    });

    tableInitializer.initTable(tableConf, executorIdSet, blockManager.getOwnershipStatus(), false);
  }

  /**
   * Associates with the table. The executors will take some blocks of this table.
   * @param executors a list of executors
   */
  public synchronized void associate(final List<AllocatedExecutor> executors) {
    final Set<String> executorIdSet = new HashSet<>(executors.size());
    executors.forEach(executor -> {
      blockManager.registerExecutor(executor.getId());
      migrationManager.registerSubscription(tableConf.getId(), executor.getId());
      executorIdSet.add(executor.getId());
    });

    tableInitializer.initTable(tableConf, executorIdSet, blockManager.getOwnershipStatus(), false);
  }

  private synchronized void associate(final String executorId) {
    final Set<String> executorIdSet = new HashSet<>(1);
    blockManager.registerExecutor(executorId);
    migrationManager.registerSubscription(tableConf.getId(), executorId);
    executorIdSet.add(executorId);

    tableInitializer.initTable(tableConf, executorIdSet, blockManager.getOwnershipStatus(), false);
  }

  /**
   * Moves the {@code numBlocks} number of blocks from src executor to dst executor.
   * @param srcExecutorId an id of src executor
   * @param dstExecutorId an id of dst executor
   * @param numBlocks the number of blocks to move
   * @param callback a callback for the result of of migration
   */
  public synchronized void moveBlocks(final String srcExecutorId,
                                      final String dstExecutorId,
                                      final int numBlocks,
                                      @Nullable final EventHandler<MigrationResult> callback) {
    // if it's not associated explicitly, do it now
    if (!blockManager.getAssociatedExecutorIds().contains(dstExecutorId)) {
      associate(dstExecutorId);
    }

    final List<Integer> blocks = blockManager.chooseBlocksToMove(srcExecutorId, numBlocks);

    // Check early failure conditions:
    // there is no block to move (maybe all blocks are moving).
    if (blocks.size() == 0) {
      final String msg = String.format("There is no block to move in %s of type." +
          " Requested numBlocks: %d", srcExecutorId, numBlocks);
      if (callback != null) {
        callback.onNext(new MigrationResult(false, msg, Collections.emptyList()));
      }
      LOG.log(Level.WARNING, "moveBlocks() fails because executor {0} has no movable block", srcExecutorId);
      return;
    }

    migrationManager.startMigration(blockManager, tableConf.getId(), srcExecutorId, dstExecutorId, blocks, callback);
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
    return blockManager.getAssociatedExecutorIds();
  }
}
