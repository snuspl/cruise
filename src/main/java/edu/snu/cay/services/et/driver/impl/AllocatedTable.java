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
import edu.snu.cay.utils.StateMachine;
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
  private final TableControlAgent tableControlAgent;

  private StateMachine stateMachine;

  private TableConfiguration tableConf;

  private enum State {
    UNINITIALIZED,
    INITIALIZED,
    DROPPED
  }

  @Inject
  private AllocatedTable(final BlockManager blockManager,
                         final MigrationManager migrationManager,
                         final TableControlAgent tableControlAgent) {
    this.blockManager = blockManager;
    this.migrationManager = migrationManager;
    this.tableControlAgent = tableControlAgent;
    this.stateMachine = initStateMachine();
  }

  private StateMachine initStateMachine() {
    return StateMachine.newBuilder()
        .addState(State.UNINITIALIZED, "Table is not initialized. It only has configuration info.")
        .addState(State.INITIALIZED, "Table is initialized. Blocks are distributed in executors.")
        .addState(State.DROPPED, "Table is dropped. All blocks and contents are deleted.")
        .addTransition(State.UNINITIALIZED, State.INITIALIZED, "Table is initialized with initial executors")
        .addTransition(State.INITIALIZED, State.DROPPED, "Table is deleted")
        .setInitialState(State.UNINITIALIZED)
        .build();
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
    stateMachine.checkState(State.UNINITIALIZED);

    tableConf = tableConfiguration;

    final Set<String> executorIds = new HashSet<>(initialAssociators.size());
    initialAssociators.forEach(executor -> {
      executorIds.add(executor.getId());
      migrationManager.registerSubscription(tableConf.getId(), executor.getId());
    });

    // partition table into blocks and initialize them in associators
    blockManager.init(executorIds);
    tableControlAgent.initTable(tableConfiguration, executorIds, blockManager.getOwnershipStatus(), true);

    stateMachine.setState(State.INITIALIZED);
  }

  /**
   * Subscribes the table. The executors will receive the updates in ownership information for this table.
   * @param executors a list of executors
   */
  public synchronized void subscribe(final List<AllocatedExecutor> executors) {
    stateMachine.checkState(State.INITIALIZED);

    final Set<String> executorIdSet = new HashSet<>(executors.size());
    executors.forEach(executor -> {
      migrationManager.registerSubscription(tableConf.getId(), executor.getId());
      executorIdSet.add(executor.getId());
    });

    tableControlAgent.initTable(tableConf, executorIdSet, blockManager.getOwnershipStatus(), false);
  }

  /**
   * Associates with the table. The executors will take some blocks of this table.
   * @param executors a list of executors
   */
  public synchronized void associate(final List<AllocatedExecutor> executors) {
    final Set<String> executorIdSet = new HashSet<>(executors.size());
    executors.forEach(executor -> executorIdSet.add(executor.getId()));

    associate(executorIdSet);
  }

  private synchronized void associate(final Set<String> executorIdSet) {
    stateMachine.checkState(State.INITIALIZED);

    executorIdSet.forEach(executorId -> {
      blockManager.registerExecutor(executorId);
      migrationManager.registerSubscription(tableConf.getId(), executorId);
      executorIdSet.add(executorId);
    });

    tableControlAgent.initTable(tableConf, executorIdSet, blockManager.getOwnershipStatus(), false);
  }

  private synchronized void associate(final String executorId) {
    final Set<String> executorIdSet = new HashSet<>(1);
    executorIdSet.add(executorId);

    associate(executorIdSet);
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
    stateMachine.checkState(State.INITIALIZED);

    // if it's not associated with dst executor, do it now
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
   * Drops {@link this} table by removing tablets and table metadata from all executors.
   * This method should be called after initialized.
   * After this method, the table is completely removed from the system (e.g., master and executors).
   */
  public synchronized void drop() {
    stateMachine.checkState(State.INITIALIZED);

    final Set<String> executorsToDeleteTable = blockManager.getAssociatedExecutorIds();
    final Set<String> subscribers = migrationManager.unregisterSubscribers(tableConf.getId());

    executorsToDeleteTable.addAll(subscribers);

    tableControlAgent.dropTable(tableConf.getId(), executorsToDeleteTable);

    stateMachine.setState(State.DROPPED);
  }

  /**
   * @return a configuration of the table
   */
  TableConfiguration getTableConfiguration() {
    return tableConf;
  }

  /**
   * @return a set of executors associated with the table
   */
  Set<String> getAssociatedExecutorIds() {
    return blockManager.getAssociatedExecutorIds();
  }
}
