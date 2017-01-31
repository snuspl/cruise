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
package edu.snu.cay.services.et.evaluator.impl;

import edu.snu.cay.common.dataloader.HdfsDataSet;
import edu.snu.cay.services.et.configuration.parameters.ExecutorIdentifier;
import edu.snu.cay.services.et.configuration.parameters.FilePath;
import edu.snu.cay.services.et.configuration.parameters.TableIdentifier;
import edu.snu.cay.services.et.evaluator.api.Table;
import edu.snu.cay.services.et.evaluator.api.TableAccessor;
import edu.snu.cay.services.et.evaluator.api.TableComponents;
import edu.snu.cay.services.et.exceptions.TableNotExistException;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.Optional;

import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A class for storing {@link Table}s that are associated with or subscribed by this executor.
 */
@Private
public final class Tables implements TableAccessor {
  private static final Logger LOG = Logger.getLogger(Tables.class.getName());

  private Map<String, Table> tableMap = new ConcurrentHashMap<>();

  private final Injector tableBaseInjector;
  private final LocalKeyGenerator localKeyGenerator;
  private final String executorId;

  @Inject
  private Tables(final Injector tableBaseInjector,
                 final LocalKeyGenerator localKeyGenerator,
                 @Parameter(ExecutorIdentifier.class) final String executorId) {
    this.tableBaseInjector = tableBaseInjector;
    this.localKeyGenerator = localKeyGenerator;
    this.executorId = executorId;
  }

  @Override
  public synchronized Table get(final String tableId) throws TableNotExistException {
    final Table table = tableMap.get(tableId);

    if (table == null) {
      // cannot access such table.
      // 1) table does not exist or 2) it's not associated or subscribed by this executor.
      throw new TableNotExistException();
    }

    return table;
  }

  /**
   * Initialize a table with given metadata.
   * @param tableConf a table configuration
   * @param blockOwnerList a block ownership status
   * @param revision a revision number of ownership status
   * @param serializedHdfsSplitInfo an Optional with a serialized hdfsSplitInfo to load
   * @return a table Id
   * @throws InjectionException
   * @throws IOException
   */
  public synchronized String initTable(final Configuration tableConf,
                                     final List<CharSequence> blockOwnerList,
                                     final int revision,
                                     final Optional<String> serializedHdfsSplitInfo)
      throws InjectionException, IOException {
    final Injector tableInjector = tableBaseInjector.forkInjector(tableConf);
    final String tableId = tableInjector.getNamedInstance(TableIdentifier.class);

    if (tableMap.containsKey(tableId)) {
      throw new RuntimeException("Table has already been initialized. tableId: " + tableId);
    }

    LOG.log(Level.INFO, "Initialize a table. tableId: {0}", new Object[]{tableConf});

    final Table table = tableInjector.getInstance(Table.class);
    final TableComponents tableComponents = tableInjector.getInstance(TableComponents.class);

    final List<String> blockOwnerStringList = new ArrayList<>(blockOwnerList.size());
    blockOwnerList.forEach(blockOwner -> blockOwnerStringList.add(blockOwner.toString()));
    // setup ownership cache of a table
    tableComponents.getOwnershipCache().init(blockOwnerStringList, revision);

    // put new local blocks into blockstore of a table
    for (int blockId = 0; blockId < blockOwnerList.size(); blockId++) {
      if (blockOwnerList.get(blockId).equals(executorId)) { // it local block
        tableComponents.getBlockStore().putBlock(blockId, new ConcurrentHashMap());
      }
    }

    // load a file into table.
    if (serializedHdfsSplitInfo.isPresent()) {
      final String filePath = tableInjector.getNamedInstance(FilePath.class);
      LOG.log(Level.INFO, "Load a file split into table. filePath: {0} tableId: {1}",
          new Object[]{filePath, tableId});
      final HdfsDataSet<?, ?> hdfsDataSet = HdfsDataSet.from(serializedHdfsSplitInfo.get());

      hdfsDataSet.forEach(pair -> table.put(localKeyGenerator.get(), pair.getValue()));
    }

    tableMap.put(tableId, table);
    return tableId;
  }
}
