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

import edu.snu.cay.services.et.common.util.concurrent.CompletedFuture;
import edu.snu.cay.services.et.common.util.concurrent.ListenableFuture;
import edu.snu.cay.services.et.configuration.TableConfiguration;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.exceptions.NotAssociatedException;
import edu.snu.cay.utils.StateMachine;
import org.apache.reef.annotations.audience.DriverSide;

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
   * @return a {@link ListenableFuture} for notifying the completion
   */
  synchronized ListenableFuture<?> init(final TableConfiguration tableConfiguration,
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

    final ListenableFuture<?> initResultFuture =
        tableControlAgent.initTable(tableConfiguration, executorIds, blockManager.getOwnershipStatus(), true);
    initResultFuture.addListener(result -> stateMachine.setState(State.INITIALIZED));
    return initResultFuture;
  }

  /**
   * Subscribes the table. The executors will receive the updates in ownership information for this table.
   * @param executors a list of executors
   */
  public synchronized ListenableFuture<?> subscribe(final List<AllocatedExecutor> executors) {
    stateMachine.checkState(State.INITIALIZED);

    final Set<String> executorIdSet = new HashSet<>(executors.size());
    executors.forEach(executor -> {
      migrationManager.registerSubscription(tableConf.getId(), executor.getId());
      executorIdSet.add(executor.getId());
    });

    return tableControlAgent.initTable(tableConf, executorIdSet, blockManager.getOwnershipStatus(), false);
  }

  /**
   * Unsubscribes the table. The executor will not receive ownership update of table anymore.
   * @param executorId an executor id
   */
  public synchronized ListenableFuture<?> unsubscribe(final String executorId) {
    stateMachine.checkState(State.INITIALIZED);

    migrationManager.unregisterSubscription(tableConf.getId(), executorId);

    return tableControlAgent.dropTable(tableConf.getId(), Collections.singleton(executorId));
  }

  /**
   * Associates with the table. The executors will take some blocks of this table.
   * @param executors a list of executors
   */
  public synchronized ListenableFuture<?> associate(final List<AllocatedExecutor> executors) {
    final Set<String> executorIdSet = new HashSet<>(executors.size());
    executors.forEach(executor -> executorIdSet.add(executor.getId()));

    return associate(executorIdSet);
  }

  private synchronized ListenableFuture<?> associate(final Set<String> executorIdSet) {
    stateMachine.checkState(State.INITIALIZED);

    executorIdSet.forEach(executorId -> {
      blockManager.registerExecutor(executorId);
      migrationManager.registerSubscription(tableConf.getId(), executorId);
      executorIdSet.add(executorId);
    });

    return tableControlAgent.initTable(tableConf, executorIdSet, blockManager.getOwnershipStatus(), false);
  }

  private synchronized ListenableFuture<?> associate(final String executorId) {
    final Set<String> executorIdSet = new HashSet<>(1);
    executorIdSet.add(executorId);

    return associate(executorIdSet);
  }

  /**
   * Decouples the table from the executor. As a result, the executor will not have any blocks nor receive any updates
   * of ownership information.
   * Note that all blocks of the table should be emptied out first, before this method is called.
   * @param executorId id of the executor to un-associate with the table
   */
  public synchronized ListenableFuture<?> unassociate(final String executorId) {
    stateMachine.checkState(State.INITIALIZED);

    blockManager.deregisterExecutor(executorId);
    migrationManager.unregisterSubscription(tableConf.getId(), executorId);

    return tableControlAgent.dropTable(tableConf.getId(), Collections.singleton(executorId));
  }

  /**
   * Moves the {@code numBlocks} number of blocks from src executor to dst executor.
   * @param srcExecutorId an id of src executor
   * @param dstExecutorId an id of dst executor
   * @param numBlocks the number of blocks to move
   */
  public synchronized ListenableFuture<MigrationResult> moveBlocks(final String srcExecutorId,
                                                                   final String dstExecutorId,
                                                                   final int numBlocks) throws NotAssociatedException {
    stateMachine.checkState(State.INITIALIZED);

    // if it's not associated with dst executor, do it now
    if (!blockManager.getPartitionInfo().containsKey(dstExecutorId)) {
      throw new NotAssociatedException(tableConf.getId(), dstExecutorId);
    }

    final List<Integer> blocks = blockManager.chooseBlocksToMove(srcExecutorId, numBlocks);

    // Check early failure conditions:
    // there is no block to move (maybe all blocks are moving).
    if (blocks.size() == 0) {
      final String msg = String.format("There is no block to move in %s of type." +
          " Requested numBlocks: %d", srcExecutorId, numBlocks);

      LOG.log(Level.WARNING, "moveBlocks() fails because executor {0} has no movable block", srcExecutorId);
      return new CompletedFuture<>(new MigrationResult(false, msg, Collections.emptyList()));
    }

    return migrationManager.startMigration(blockManager, tableConf.getId(), srcExecutorId, dstExecutorId, blocks);
  }

  /**
   * Returns a partition information of this table.
   * @return a map between an executor id and a set of block ids
   */
  public Map<String, Set<Integer>> getPartitionInfo() {
    return blockManager.getPartitionInfo();
  }

  /**
   * Drops {@link this} table by removing tablets and table metadata from all executors.
   * This method should be called after initialized.
   * After this method, the table is completely removed from the system (e.g., master and executors).
   */
  public synchronized ListenableFuture<?> drop() {
    stateMachine.checkState(State.INITIALIZED);

    final Set<String> associators = blockManager.getPartitionInfo().keySet();
    final Set<String> subscribers = migrationManager.unregisterSubscribers(tableConf.getId());

    final Set<String> executorsToDeleteTable = new HashSet<>(associators);
    executorsToDeleteTable.addAll(subscribers);

    final ListenableFuture<?> dropResultFuture =
        tableControlAgent.dropTable(tableConf.getId(), executorsToDeleteTable);
    dropResultFuture.addListener(result -> stateMachine.setState(State.DROPPED));
    return dropResultFuture;
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
    return blockManager.getPartitionInfo().keySet();
  }
}
