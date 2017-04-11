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

import edu.snu.cay.services.et.configuration.parameters.ExecutorIdentifier;
import edu.snu.cay.services.et.configuration.parameters.FilePath;
import edu.snu.cay.services.et.configuration.parameters.IsOrderedTable;
import edu.snu.cay.services.et.configuration.parameters.TableIdentifier;
import edu.snu.cay.services.et.evaluator.api.Table;
import edu.snu.cay.services.et.evaluator.api.TableAccessor;
import edu.snu.cay.services.et.exceptions.BlockAlreadyExistsException;
import edu.snu.cay.services.et.exceptions.KeyGenerationException;
import edu.snu.cay.services.et.exceptions.TableNotExistException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Executor-side component for managing initialized tables.
 */
@EvaluatorSide
@ThreadSafe
@Private
public final class Tables implements TableAccessor {
  private static final Logger LOG = Logger.getLogger(Tables.class.getName());

  private final Injector tableBaseInjector;
  private final String executorId;

  /**
   * A mapping with tableIds and the corresponding {@link TableImpl}s.
   */
  private final Map<String, Pair<Table, TableComponents>> tables = new ConcurrentHashMap<>();

  @Inject
  private Tables(final Injector tableBaseInjector,
                 @Parameter(ExecutorIdentifier.class) final String executorId,
                 final RemoteAccessOpHandler remoteAccessOpHandler,
                 final RemoteAccessOpSender remoteAccessOpSender,
                 final MigrationExecutor migrationExecutor) {
    this.tableBaseInjector = tableBaseInjector;
    this.executorId = executorId;

    // RemoteAccessOpHandler/Sender and MigrationExecutor should be instantiated although they're not actually accessed.
    // This is intentional. Otherwise RemoteAccessOpHandler/Sender and MigrationExecutor are created per Table,
    // which we want to keep singleton.
  }

  /**
   * Initialize a local table.
   * @param tableConf Tang configuration for this table
   * @param blockOwners a blockId-to-executorId map for remote operation routing
   * @return identifier of the initialized table
   * @throws InjectionException Table configuration is incomplete to initialize a table
   */
  public synchronized String initTable(final Configuration tableConf,
                                       final List<String> blockOwners) throws InjectionException {
    final Injector tableInjector = tableBaseInjector.forkInjector(tableConf);
    final String tableId = tableInjector.getNamedInstance(TableIdentifier.class);
    if (tables.containsKey(tableId)) {
      throw new RuntimeException("Table has already been initialized. tableId: " + tableId);
    }

    // Initialize a table
    LOG.log(Level.INFO, "Initializing a table. tableId: {0}", tableId);
    final TableComponents tableComponents = tableInjector.getInstance(TableComponents.class);
    initTableComponents(tableComponents, blockOwners);

    final TableImpl table = tableInjector.getInstance(TableImpl.class);
    tables.put(tableId, Pair.of(table, tableComponents));
    return tableId;
  }

  /**
   * Initialize a local table, loading data from a file specified by {@code serializedHdfsSplitInfo}.
   * @param tableConf Tang configuration for this table
   * @param blockOwners a blockId-to-executorId map for remote operation routing
   * @param serializedHdfsSplitInfo resource identifier for bulk-loading
   * @return identifier of the initialized table
   * @throws InjectionException Table configuration is incomplete to initialize a table
   */
  @SuppressWarnings("unchecked")
  public synchronized String initTable(final Configuration tableConf,
                                       final List<String> blockOwners,
                                       final String serializedHdfsSplitInfo) throws InjectionException {
    final Injector tableInjector = tableBaseInjector.forkInjector(tableConf);
    final String tableId = tableInjector.getNamedInstance(TableIdentifier.class);
    if (tables.containsKey(tableId)) {
      throw new RuntimeException("Table has already been initialized. tableId: " + tableId);
    }

    // Initialize a table
    LOG.log(Level.INFO, "Initializing a table. tableId: {0}", tableId);
    final TableComponents tableComponents = tableInjector.getInstance(TableComponents.class);

    initTableComponents(tableComponents, blockOwners);

    // Load a file into table
    if (serializedHdfsSplitInfo != null) {
      final String filePath = tableInjector.getNamedInstance(FilePath.class);
      LOG.log(Level.INFO, "Load a file split into table. filePath: {0} tableId: {1}",
          new Object[]{filePath, tableId});

      final boolean isOrderedTable = tableInjector.getNamedInstance(IsOrderedTable.class);
      if (isOrderedTable) {
        final BulkDataLoader bulkDataLoader = tableInjector.getInstance(BulkDataLoader.class);
        try {
          bulkDataLoader.load(serializedHdfsSplitInfo);
        } catch (KeyGenerationException | IOException e) {
          throw new RuntimeException("Fail to load a file into a table", e);
        }

      } else {
        // TODO #52: Support bulk data(file) loading for hashed tables
        LOG.log(Level.WARNING, "File loading is not available for hashed tables. tableId: {0}", tableId);
      }
    }

    final Table table = tableInjector.getInstance(Table.class);
    tables.put(tableId, Pair.of(table, tableComponents));
    return tableId;
  }

  /**
   * Initialize table components such as {@link OwnershipCache} and {@link BlockStore}.
   */
  private void initTableComponents(final TableComponents tableComponents,
                                   final List<String> blockOwners) throws InjectionException {
    // Initialize ownership cache
    tableComponents.getOwnershipCache().init(blockOwners);

    // Create local blocks if needed
    try {
      for (int blockId = 0; blockId < blockOwners.size(); blockId++) {
        if (blockOwners.get(blockId).equals(executorId)) {
          tableComponents.getBlockStore().createEmptyBlock(blockId);
        }
      }
    } catch (BlockAlreadyExistsException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Remove a table from this executor and a local tablet.
   * @param tableId the identifier of the the table
   */
  public synchronized void remove(final String tableId) {
    if (!tables.containsKey(tableId)) {
      throw new RuntimeException(tableId + " does not exist");
    }

    // remove table and clear contents in local tablet
    // it assumes that there's no ongoing migration
    final Table removedTable = tables.remove(tableId).getLeft();
    removedTable.getLocalTablet().clear();
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized <K, V, U> Table<K, V, U> getTable(final String tableId) throws TableNotExistException {
    final Pair<Table, TableComponents> tablePair = tables.get(tableId);
    if (tablePair == null) {
      // cannot access such table.
      // 1) table does not exist or 2) it's not associated or subscribed by this executor.
      throw new TableNotExistException();
    }

    return tablePair.getLeft();
  }

  /**
   * Return a {@link TableComponents}, which contains all internal components of a table.
   * Note that this method is used by the system internally and should not be exposed to users.
   * @param tableId the identifier of the table
   * @return the corresponding {@link TableComponents} object
   * @throws TableNotExistException when there's no table with the specified id
   */
  @SuppressWarnings("unchecked")
  synchronized <K, V, U> TableComponents<K, V, U> getTableComponents(final String tableId)
      throws TableNotExistException {
    final Pair<Table, TableComponents> tablePair = tables.get(tableId);
    if (tablePair == null) {
      // cannot access such table.
      // 1) table does not exist or 2) it's not associated or subscribed by this executor.
      throw new TableNotExistException();
    }
    return tablePair.getRight();
  }

  /**
   * @return the number of blocks of each Table.
   */
  public synchronized Map<String, Integer> getTableToNumBlocks() {
    final Map<String, Integer> tableIdToNumBlocks = new HashMap<>();

    tables.keySet().forEach(tableId -> {
          try {
            final TableComponents components = getTableComponents(tableId);
            tableIdToNumBlocks.put(tableId, components.getBlockStore().getNumBlocks());
          } catch (TableNotExistException e) {
            throw new RuntimeException(e);
          }
        }
    );
    return tableIdToNumBlocks;
  }
}
