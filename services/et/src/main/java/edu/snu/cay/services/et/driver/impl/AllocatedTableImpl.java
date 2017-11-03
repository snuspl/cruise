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
import edu.snu.cay.services.et.common.util.concurrent.ResultFuture;
import edu.snu.cay.services.et.configuration.TableConfiguration;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.AllocatedTable;
import edu.snu.cay.services.et.exceptions.NotAssociatedException;
import edu.snu.cay.utils.StateMachine;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * An implementation of {@link AllocatedTable}.
 * {@link #init(TableConfiguration, List)} should be called only once after instantiating this object.
 */
@DriverSide
@Private
public final class AllocatedTableImpl implements AllocatedTable {
  private static final Logger LOG = Logger.getLogger(AllocatedTableImpl.class.getName());

  private final BlockManager blockManager;
  private final MigrationManager migrationManager;
  private final ChkpManagerMaster chkpManagerMaster;
  private final SubscriptionManager subscriptionManager;
  private final TableControlAgent tableControlAgent;
  private final TableManager tableManager;

  private final AtomicInteger numOngoingMigrations = new AtomicInteger(0);
  private final AtomicInteger numOngoingCheckpoints = new AtomicInteger(0);

  private StateMachine stateMachine;

  private TableConfiguration tableConf;

  private enum State {
    UNINITIALIZED,
    INITIALIZED,
    DROPPED
  }

  @Inject
  private AllocatedTableImpl(final BlockManager blockManager,
                             final MigrationManager migrationManager,
                             final ChkpManagerMaster chkpManagerMaster,
                             final SubscriptionManager subscriptionManager,
                             final TableControlAgent tableControlAgent,
                             final TableManager tableManager) {
    this.blockManager = blockManager;
    this.migrationManager = migrationManager;
    this.chkpManagerMaster = chkpManagerMaster;
    this.subscriptionManager = subscriptionManager;
    this.tableControlAgent = tableControlAgent;
    this.tableManager = tableManager;
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

  @Override
  public synchronized ListenableFuture<?> init(final TableConfiguration tableConfiguration,
                                               final List<AllocatedExecutor> initialAssociators) {
    stateMachine.checkState(State.UNINITIALIZED);
    tableConf = tableConfiguration;

    final Set<String> executorIds = new HashSet<>(initialAssociators.size());
    initialAssociators.forEach(executor -> {
      executorIds.add(executor.getId());
      subscriptionManager.registerSubscription(tableConf.getId(), executor.getId());
    });

    // partition table into blocks and initialize them in associators
    blockManager.init(executorIds);

    final ListenableFuture<?> initResultFuture =
        tableControlAgent.initTable(tableConf, executorIds, blockManager.getOwnershipStatus());

    initResultFuture.addListener(result -> stateMachine.setState(State.INITIALIZED));
    return initResultFuture;
  }

  @Override
  public synchronized ListenableFuture<?> load(final List<AllocatedExecutor> executors, final String inputPath) {

    stateMachine.checkState(State.INITIALIZED);
    final Set<String> executorIdSet = new HashSet<>(executors.size());
    executors.forEach(executor -> executorIdSet.add(executor.getId()));

    return tableControlAgent.load(tableConf.getId(), executorIdSet, inputPath);
  }

  @Override
  public synchronized ListenableFuture<?> subscribe(final List<AllocatedExecutor> executors) {
    stateMachine.checkState(State.INITIALIZED);

    final Set<String> executorIdSet = executors.stream().map(AllocatedExecutor::getId).collect(Collectors.toSet());

    final int opId = subscriptionManager.startSubscriptionInit(tableConf.getId(), executorIdSet);

    final ListenableFuture<?> future = tableControlAgent.initTable(tableConf, executorIdSet,
        blockManager.getOwnershipStatus());

    future.addListener(o -> subscriptionManager.finishSubscriptionInit(opId));

    return future;
  }

  @Override
  public synchronized ListenableFuture<?> unsubscribe(final String executorId) {
    stateMachine.checkState(State.INITIALIZED);

    final ListenableFuture<?> future = tableControlAgent.dropTable(
        tableConf.getId(), Collections.singleton(executorId));
    future.addListener(o -> subscriptionManager.unregisterSubscription(tableConf.getId(), executorId));

    return future;
  }

  @Override
  public synchronized ListenableFuture<?> associate(final List<AllocatedExecutor> executors) {
    final Set<String> executorIdSet = new HashSet<>(executors.size());
    executors.forEach(executor -> executorIdSet.add(executor.getId()));

    return associate(executorIdSet);
  }

  private synchronized ListenableFuture<?> associate(final Set<String> executorIdSet) {
    stateMachine.checkState(State.INITIALIZED);

    final int opId = subscriptionManager.startSubscriptionInit(tableConf.getId(), executorIdSet);

    final ListenableFuture<?> future = tableControlAgent.initTable(tableConf, executorIdSet,
        blockManager.getOwnershipStatus());

    future.addListener(o -> {
      executorIdSet.forEach(blockManager::registerExecutor);
      subscriptionManager.finishSubscriptionInit(opId);
    });

    return future;
  }

  private synchronized ListenableFuture<?> associate(final String executorId) {
    final Set<String> executorIdSet = new HashSet<>(1);
    executorIdSet.add(executorId);

    return associate(executorIdSet);
  }

  @Override
  public synchronized ListenableFuture<?> unassociate(final String executorId) {
    stateMachine.checkState(State.INITIALIZED);

    final ResultFuture<Void> resultFuture = new ResultFuture<>();

    // sync ownership caches in all executors that can access the table
    final Set<String> executorsToSync = subscriptionManager.getSubscribers(tableConf.getId());
    executorsToSync.remove(executorId);

    // do actual unassociation after sync
    tableControlAgent.syncOwnership(tableConf.getId(), executorId, executorsToSync)
        .addListener(o -> tableControlAgent.dropTable(tableConf.getId(), Collections.singleton(executorId))
            .addListener(o1 -> {
              blockManager.deregisterExecutor(executorId);
              subscriptionManager.unregisterSubscription(tableConf.getId(), executorId);
              resultFuture.onCompleted(null);
            }));

    return resultFuture;
  }

  @Override
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

    // can proceed only when there's no ongoing checkpoints
    while (numOngoingCheckpoints.get() > 0) {
      try {
        LOG.log(Level.INFO, "Wait until ongoing checkpoints are completed.");
        wait();
      } catch (InterruptedException e) {
        // ignore
      }
    }

    numOngoingMigrations.incrementAndGet();

    final ListenableFuture<MigrationResult> future =
        migrationManager.startMigration(blockManager, tableConf.getId(), srcExecutorId, dstExecutorId, blocks);
    future.addListener(o -> {
      if (numOngoingMigrations.decrementAndGet() == 0) {
        synchronized (this) {
          notifyAll();
        }
      }
    });

    return future;
  }

  @Override
  public Map<String, Set<Integer>> getPartitionInfo() {
    return blockManager.getPartitionInfo();
  }

  @Override
  public List<String> getOwnershipStatus() {
    return blockManager.getOwnershipStatus();
  }

  @Override
  public String getOwnerId(final int blockId) {
    return blockManager.getOwnerId(blockId);
  }

  @Override
  public synchronized ListenableFuture<?> drop() {
    stateMachine.checkState(State.INITIALIZED);

    final Set<String> associators = blockManager.getAssociatorIds();
    final Set<String> subscribers = subscriptionManager.unregisterSubscribers(tableConf.getId());

    final Set<String> executorsToDeleteTable = new HashSet<>(associators);
    executorsToDeleteTable.addAll(subscribers);

    final ResultFuture<?> future = new ResultFuture<>();

    final ListenableFuture<?> dropResultFuture =
        tableControlAgent.dropTable(tableConf.getId(), executorsToDeleteTable);
    dropResultFuture.addListener(result -> {
      stateMachine.setState(State.DROPPED);
      tableManager.onDropTable(tableConf.getId());
      future.onCompleted(null);
    });

    return future;
  }

  @Override
  public synchronized ListenableFuture<String> checkpoint() {
    return checkpoint(1.0);
  }

  @Override
  public ListenableFuture<String> checkpoint(final double samplingRatio) {
    stateMachine.checkState(State.INITIALIZED);

    if (samplingRatio > 1 || samplingRatio <= 0) {
      throw new IllegalArgumentException("Sample ratio should be a positive value that is less than and equal to 1.");
    }

    // can proceed only when there's no ongoing checkpoints
    while (numOngoingMigrations.get() > 0) {
      try {
        LOG.log(Level.INFO, "Wait until ongoing migrations are completed.");
        wait();
      } catch (InterruptedException e) {
        // ignore
      }
    }

    numOngoingCheckpoints.incrementAndGet();

    final ListenableFuture<String> future =
        chkpManagerMaster.checkpoint(tableConf, blockManager.getAssociatorIds(), samplingRatio);
    future.addListener(o -> {
      if (numOngoingCheckpoints.decrementAndGet() == 0) {
        synchronized (this) {
          notifyAll();
        }
      }
    });

    return future;
  }

  @Override
  public String getId() {
    return tableConf.getId();
  }

  @Override
  public TableConfiguration getTableConfiguration() {
    return tableConf;
  }

  @Override
  public Set<String> getAssociatedExecutorIds() {
    return blockManager.getAssociatorIds();
  }
}
