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

import edu.snu.cay.common.dataloader.HdfsSplitInfo;
import edu.snu.cay.common.dataloader.HdfsSplitManager;
import edu.snu.cay.common.dataloader.TextInputFormat;
import edu.snu.cay.services.et.common.util.concurrent.ListenableFuture;
import edu.snu.cay.services.et.common.util.concurrent.AggregateFuture;
import edu.snu.cay.services.et.configuration.TableConfiguration;
import edu.snu.cay.services.et.driver.api.MessageSender;
import org.apache.reef.annotations.audience.DriverSide;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An agent class that takes charge of initializing and deleting tables at executors.
 * Initialization installs table metadata in executors; it includes basic immutable table configurations
 * (e.g., key/value codec, partition function, num total blocks) and mutable ownership status.
 * Drop operation removes the above metadata and tablets that contain actual data from executors.
 */
@DriverSide
final class TableControlAgent {
  private static final Logger LOG = Logger.getLogger(TableControlAgent.class.getName());

  private final AtomicLong operationIdCounter = new AtomicLong(0);

  private final MessageSender msgSender;

  private final Map<Long, AggregateFuture<Void>> pendingInit = new ConcurrentHashMap<>();
  private final Map<Long, AggregateFuture<Void>> pendingLoad = new ConcurrentHashMap<>();
  private final Map<Long, AggregateFuture<Void>> pendingDrop = new ConcurrentHashMap<>();
  private final Map<Long, AggregateFuture<Void>> pendingSync = new ConcurrentHashMap<>();

  @Inject
  private TableControlAgent(final MessageSender msgSender) {
    this.msgSender = msgSender;
  }

  /**
   * Initializes a table in executors by providing table metadata.
   * It's a blocking call so that waits until all executors setup the table.
   * @param tableConf a configuration of table
   * @param executorIds a set of executor ids
   * @param ownershipStatus a list of owner of each block
   */
  ListenableFuture<?> initTable(final TableConfiguration tableConf,
                                final Set<String> executorIds,
                                final List<String> ownershipStatus) {
    final long opId = operationIdCounter.getAndIncrement();
    LOG.log(Level.INFO, "Initialize table {0} in executors: {1}. opId: {2}",
        new Object[]{tableConf.getId(), executorIds, opId});

    final AggregateFuture<Void> resultFuture = new AggregateFuture<>(executorIds.size());
    pendingInit.put(opId, resultFuture);

    executorIds.forEach(executorId ->
        msgSender.sendTableInitMsg(opId, executorId, tableConf, ownershipStatus));

    return resultFuture;
  }

  /**
   * Marks that a table initialization started by {@link #initTable} has been done in an executor.
   * @param tableId a table id
   * @param executorId an executor id
   */
  synchronized void onTableInitAck(final long opId, final String tableId, final String executorId) {
    LOG.log(Level.INFO, "Table {0} in executor {1} is initialized.",
        new Object[]{tableId, executorId});
    final AggregateFuture<Void> resultFuture = pendingInit.get(opId);
    if (resultFuture == null) {
      throw new RuntimeException("There's no ongoing init of table. opId: " + opId);
    } else if (resultFuture.isDone()) {
      throw new RuntimeException("The init operation was already handled. opId: " + opId);
    }

    resultFuture.onCompleted(null);

    if (resultFuture.isDone()) {
      pendingInit.remove(opId);
    }
  }

  /**
   * Loads data in a file to a table, by assigning a file split to each executor.
   * Executors will read data from a given split and put into the table.
   * @param tableId a identifier of a table
   * @param executorIds a set of executor id
   * @param inputPath a file path the data are located
   */
  ListenableFuture<?> load(final String tableId,
                           final Set<String> executorIds,
                           final String inputPath) {
    final long opId = operationIdCounter.getAndIncrement();
    LOG.log(Level.INFO, "Load table data {0} in executors: {1}. opId: {2}",
        new Object[]{tableId, executorIds, opId});

    // let's assume that tables always use this TextInputFormat class,
    // which always returns the exact number of splits as requested
    final HdfsSplitInfo[] fileSplits = HdfsSplitManager.getSplits(inputPath,
        TextInputFormat.class.getName(), executorIds.size());
    LOG.log(Level.INFO, "The number of data splits : {0}", fileSplits.length);

    final Iterator<HdfsSplitInfo> splitIterator = Arrays.asList(fileSplits).iterator();

    final AggregateFuture<Void> resultFuture = new AggregateFuture<>(executorIds.size());
    pendingLoad.put(opId, resultFuture);

    executorIds.forEach(executorId -> msgSender.sendTableLoadMsg(opId, executorId, tableId, splitIterator.next()));

    return resultFuture;
  }

  /**
   * Marks that a table load started by {@link #load} has been done in an executor.
   * @param tableId an identifier of table
   * @param executorId and identifier of executor
   */
  synchronized void onTableLoadAck(final long opId, final String tableId, final String executorId) {
    LOG.log(Level.INFO, "Table {0} in executor {1} loaded data.",
        new Object[]{tableId, executorId});
    final AggregateFuture<Void> resultFuture = pendingLoad.get(opId);
    if (resultFuture == null) {
      throw new RuntimeException("There's no ongoing load data of table. opId: " + opId);
    } else if (resultFuture.isDone()) {
      throw new RuntimeException("The load operation was already handled. opId: " + opId);
    }

    resultFuture.onCompleted(null);

    if (resultFuture.isDone()) {
      pendingLoad.remove(opId);
    }
  }

  /**
   * Deletes a table, which consists of metadata and tablets in executors.
   * Be aware that the contents in tablets will be lost.
   * @param tableId a table id
   * @param executorIdSet a set of executor ids
   * @return a {@link ListenableFuture} of the result
   */
  ListenableFuture<?> dropTable(final String tableId,
                                final Set<String> executorIdSet) {
    final long opId = operationIdCounter.getAndIncrement();
    LOG.log(Level.INFO, "Drop table {0} in executors: {1}. opId: {2}", new Object[]{tableId, executorIdSet, opId});

    final AggregateFuture<Void> resultFuture = new AggregateFuture<>(executorIdSet.size());
    pendingDrop.put(opId, resultFuture);

    executorIdSet.forEach(executorId -> msgSender.sendTableDropMsg(opId, executorId, tableId));

    return resultFuture;
  }

  /**
   * Marks that a table drop started by {@link #dropTable} has been done in an executor.
   * @param tableId a table id
   * @param executorId an executor id
   */
  synchronized void onTableDropAck(final long opId, final String tableId, final String executorId) {
    LOG.log(Level.INFO, "Table {0} in executor {1} is dropped.", new Object[]{tableId, executorId});
    final AggregateFuture<Void> resultFuture = pendingDrop.get(opId);
    if (resultFuture == null) {
      throw new RuntimeException("There's no ongoing drop of table. opId: " + opId);
    } else if (resultFuture.isDone()) {
      throw new RuntimeException("The drop operation was already handled. opId: " + opId);
    }

    resultFuture.onCompleted(null);

    if (resultFuture.isDone()) {
      pendingDrop.remove(opId);
    }
  }

  /**
   * Synchronize ownership status of executors in {@code executorIdSet}.
   * Specifically it confirms that they have no entry of {@code deletedExecutorId}.
   * @param tableId a table id
   * @param deletedExecutorId a deleted executor id
   * @param executorIdSet a set of executor ids to sync
   */
  ListenableFuture<?> syncOwnership(final String tableId, final String deletedExecutorId,
                                    final Set<String> executorIdSet) {
    final long opId = operationIdCounter.getAndIncrement();
    LOG.log(Level.INFO, "Sync ownership of table {0} in executors: {1}. DeletedExecutorId: {2}, OpId: {3}",
        new Object[]{tableId, executorIdSet, deletedExecutorId, opId});

    final AggregateFuture<Void> resultFuture = new AggregateFuture<>(executorIdSet.size());
    pendingSync.put(opId, resultFuture);

    executorIdSet.forEach(executorId -> msgSender.sendOwnershipSyncMsg(opId, executorId, tableId, deletedExecutorId));

    return resultFuture;
  }

  /**
   * Marks that a ownership sync started by {@link #syncOwnership} has been done in an executor.
   * @param opId an operation id
   * @param tableId a table id
   * @param executorId an executor id
   */
  synchronized void onOwnershipSyncAck(final long opId, final String tableId, final String executorId) {
    LOG.log(Level.INFO, "Ownership sync of table {0} in {1} is finished. opId: {2}",
        new Object[]{tableId, executorId, opId});
    final AggregateFuture<Void> resultFuture = pendingSync.get(opId);
    if (resultFuture == null) {
      throw new RuntimeException("There's no ongoing sync of table ownership. opId: " + opId);
    } else if (resultFuture.isDone()) {
      throw new RuntimeException("The sync operation was already handled. opId: " + opId);
    }

    resultFuture.onCompleted(null);

    if (resultFuture.isDone()) {
      pendingSync.remove(opId);
    }
  }
}
