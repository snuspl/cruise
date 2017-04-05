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

import edu.snu.cay.common.dataloader.HdfsSplitInfo;
import edu.snu.cay.common.dataloader.HdfsSplitManager;
import edu.snu.cay.common.dataloader.TextInputFormat;
import edu.snu.cay.services.et.configuration.TableConfiguration;
import edu.snu.cay.services.et.driver.api.MessageSender;
import org.apache.reef.annotations.audience.DriverSide;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
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

  private final MessageSender msgSender;

  private final Map<String, CountDownLatch> pendingInit = new ConcurrentHashMap<>();
  private final Map<String, CountDownLatch> pendingDrop = new ConcurrentHashMap<>();

  @Inject
  private TableControlAgent(final MessageSender msgSender) {
    this.msgSender = msgSender;
  }

  /**
   * Initializes a table in executors by providing table metadata.
   * For initial associators of a table, setting {@code loadFile} as true will load a file into executors
   * by assigning a split for each executor.
   * It's a blocking call so that waits until all executors setup the table.
   * @param tableConf a configuration of table
   * @param executorIds a set of executor ids
   * @param ownershipStatus a list of owner of each block
   * @param loadFile a boolean indicating whether to load a file whose path is specified in tableConf
   */
  void initTable(final TableConfiguration tableConf,
                 final Set<String> executorIds,
                 final List<String> ownershipStatus,
                 final boolean loadFile) {
    LOG.log(Level.INFO, "Initialize table {0} in executors: {1}", new Object[]{tableConf.getId(), executorIds});

    final Iterator<HdfsSplitInfo> splitIterator;

    final Optional<String> filePathOptional = tableConf.getFilePath();
    if (loadFile && filePathOptional.isPresent()) {
      final String filePath = filePathOptional.get();

      // let's assume that tables always use this TextInputFormat class
      final HdfsSplitInfo[] fileSplits = HdfsSplitManager.getSplits(filePath,
          TextInputFormat.class.getName(), executorIds.size());

      splitIterator = Arrays.asList(fileSplits).iterator();
    } else {
      splitIterator = Collections.emptyIterator();
    }

    final CountDownLatch initLatch = new CountDownLatch(executorIds.size());
    pendingInit.put(tableConf.getId(), initLatch);

    executorIds.forEach(executorId ->
        msgSender.sendTableInitMsg(executorId, tableConf, ownershipStatus,
            splitIterator.hasNext() ? splitIterator.next() : null));

    try {
      initLatch.await();
    } catch (final InterruptedException e) {
      throw new RuntimeException("Interrupted while waiting for table to be initialized.", e);
    } finally {
      pendingInit.remove(tableConf.getId());
    }
  }

  /**
   * Marks that a table initialization started by {@link #initTable} has been done in an executor.
   * @param tableId a table id
   * @param executorId an executor id
   */
  synchronized void onTableInitAck(final String tableId, final String executorId) {
    LOG.log(Level.INFO, "Table {0} in executor {1} is initialized.", new Object[]{tableId, executorId});
    final CountDownLatch initLatch = pendingInit.get(tableId);
    if (initLatch == null || initLatch.getCount() == 0) {
      throw new RuntimeException("There's no ongoing init for table. tableId: " + tableId);
    }
    initLatch.countDown();
  }

  /**
   * Deletes a table, which consists of metadata and tablets in executors.
   * Be aware that the contents in tablets will be lost.
   * @param tableId a table id
   * @param executorIdSet a set of executor ids
   */
  void dropTable(final String tableId,
                 final Set<String> executorIdSet) {
    LOG.log(Level.INFO, "Drop table {0} in executors: {1}", new Object[]{tableId, executorIdSet});

    final CountDownLatch dropLatch = new CountDownLatch(executorIdSet.size());
    pendingDrop.put(tableId, dropLatch);

    executorIdSet.forEach(executorId -> msgSender.sendTableDropMsg(executorId, tableId));

    try {
      dropLatch.await();
    } catch (final InterruptedException e) {
      throw new RuntimeException("Interrupted while waiting for table to be deleted.", e);
    } finally {
      pendingDrop.remove(tableId);
    }
  }

  /**
   * Marks that a table drop started by {@link #dropTable} has been done in an executor.
   * @param tableId a table id
   * @param executorId an executor id
   */
  synchronized void onTableDropAck(final String tableId, final String executorId) {
    LOG.log(Level.INFO, "Table {0} in executor {1} is dropped.", new Object[]{tableId, executorId});
    final CountDownLatch dropLatch = pendingDrop.get(tableId);
    if (dropLatch == null || dropLatch.getCount() == 0) {
      throw new RuntimeException("There's no ongoing drop for table. tableId: " + tableId);
    }
    dropLatch.countDown();
  }
}
