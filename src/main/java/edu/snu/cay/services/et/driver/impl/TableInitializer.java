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
 * A table initializer class that initializes table at executors.
 * Initialization info includes basic immutable table configurations
 * (e.g., key/value codec, partition function, num total blocks) and mutable ownership status.
 */
@DriverSide
final class TableInitializer {
  private static final Logger LOG = Logger.getLogger(TableInitializer.class.getName());

  private final MessageSender msgSender;

  private final Map<String, CountDownLatch> pendingInit = new ConcurrentHashMap<>();

  @Inject
  private TableInitializer(final MessageSender msgSender) {
    this.msgSender = msgSender;
  }

  /**
   * Initializes a table in executors by providing table metadata.
   * For initial associators of a table, setting {@code loadFile} as true will load a file into executors
   * by assigning a split for each executor.
   * It's a blocking call so that waits until all executors setup corresponding tablets.
   * @param tableConf a configuration of table
   * @param executorIdSet a set of executor ids
   * @param ownershipStatus a list of owner of each block
   * @param loadFile a boolean indicating whether to load a file whose path is specified in tableConf
   */
  void initTable(final TableConfiguration tableConf,
                 final Set<String> executorIdSet,
                 final List<String> ownershipStatus,
                 final boolean loadFile) {
    LOG.log(Level.INFO, "Initialize table {0} in executors: {1}", new Object[]{tableConf.getId(), executorIdSet});

    final Iterator<HdfsSplitInfo> splitIterator;

    final Optional<String> filePathOptional = tableConf.getFilePath();
    if (loadFile && filePathOptional.isPresent()) {
      final String filePath = filePathOptional.get();

      // let's assume that tables always use this TextInputFormat class
      final HdfsSplitInfo[] fileSplits = HdfsSplitManager.getSplits(filePath,
          TextInputFormat.class.getName(), executorIdSet.size());

      splitIterator = Arrays.asList(fileSplits).iterator();
    } else {
      splitIterator = Collections.emptyIterator();
    }

    final CountDownLatch initLatch = new CountDownLatch(executorIdSet.size());
    pendingInit.put(tableConf.getId(), initLatch);

    executorIdSet.forEach(executorId ->
        msgSender.sendTableInitMsg(executorId, tableConf, ownershipStatus,
            splitIterator.hasNext() ? splitIterator.next() : null));

    try {
      initLatch.await();
    } catch (final InterruptedException e) {
      throw new RuntimeException("Interrupted while waiting for table to be initialized.", e);
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
}
