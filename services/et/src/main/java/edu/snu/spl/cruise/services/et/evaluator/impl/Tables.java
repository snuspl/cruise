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
package edu.snu.spl.cruise.services.et.evaluator.impl;

import edu.snu.spl.cruise.services.et.configuration.parameters.ExecutorIdentifier;
import edu.snu.spl.cruise.services.et.configuration.parameters.TableIdentifier;
import edu.snu.spl.cruise.services.et.evaluator.api.Table;
import edu.snu.spl.cruise.services.et.evaluator.api.TableAccessor;
import edu.snu.spl.cruise.services.et.exceptions.BlockAlreadyExistsException;
import edu.snu.spl.cruise.services.et.exceptions.TableNotExistException;
import edu.snu.spl.cruise.utils.Tuple3;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
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
   * A mapping with tableIds and the corresponding {@link Table}, {@link TableComponents}, and table configuration.
   */
  private final Map<String, Tuple3<Table, TableComponents, Configuration>> tables = new ConcurrentHashMap<>();

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
    final String tableId = Tang.Factory.getTang().newInjector(tableConf).getNamedInstance(TableIdentifier.class);
    if (tables.containsKey(tableId)) {
      throw new RuntimeException("Table has already been initialized. tableId: " + tableId);
    }

    // Initialize a table
    LOG.log(Level.INFO, "Initializing a table. tableId: {0}", tableId);
    final Pair<Table, TableComponents> tablePair = instantiateTable(tableConf, blockOwners);
    final Table table = tablePair.getLeft();
    final TableComponents tableComponents = tablePair.getRight();

    tables.put(tableId, new Tuple3<>(table, tableComponents, tableConf));
    return tableId;
  }

  /**
   * Instantiates a table.
   * Note that the table instantiated by this method is not added to table entry maintained by ET.
   * @param tableConf Tang configuration for this table
   * @param blockOwners a blockId-to-executorId map for remote operation routing
   * @return a pair of {@link Table} and {@link TableComponents}
   * @throws InjectionException Table configuration is incomplete to initialize a table
   */
  public Pair<Table, TableComponents> instantiateTable(final Configuration tableConf,
                                                       final List<String> blockOwners) throws InjectionException {
    final Injector tableInjector = tableBaseInjector.forkInjector(tableConf);
    final TableComponents tableComponents = tableInjector.getInstance(TableComponents.class);
    final Table table = tableInjector.getInstance(Table.class);
    tableComponents.setTableConf(tableConf);

    // Initialize ownership cache
    tableComponents.getOwnershipCache().init(blockOwners);
    // Create local blocks
    initEmptyBlocks(tableComponents.getBlockStore(), blockOwners);
    return Pair.of(table, tableComponents);
  }

  /**
   * Initialize block store with empty local blocks.
   */
  private void initEmptyBlocks(final BlockStore blockStore,
                               final List<String> blockOwners) {
    // Create local blocks
    try {
      for (int blockId = 0; blockId < blockOwners.size(); blockId++) {
        if (blockOwners.get(blockId).equals(executorId)) {
          blockStore.createEmptyBlock(blockId);
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
  public synchronized void remove(final String tableId) throws TableNotExistException {
    if (!tables.containsKey(tableId)) {
      throw new TableNotExistException(tableId + " does not exist");
    }

    // remove table and clear contents in local tablet
    // it assumes that there's no ongoing migration
    final Table removedTable = tables.remove(tableId).getFirst();
    removedTable.getLocalTablet().clear();
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized <K, V, U> Table<K, V, U> getTable(final String tableId) throws TableNotExistException {
    final Tuple3<Table, TableComponents, Configuration> tableTuple = tables.get(tableId);
    if (tableTuple == null) {
      // cannot access such table.
      throw new TableNotExistException(tableId + " does not exist or this executor did not associate or subscribe it");
    }

    return tableTuple.getFirst();
  }

  /**
   * Return a {@link TableComponents}, which contains all internal components of a table.
   * Note that this method is used by the system internally and should not be exposed to users.
   * @param tableId the identifier of the table
   * @return the corresponding {@link TableComponents} object
   * @throws TableNotExistException when there's no table with the specified id
   */
  @SuppressWarnings("unchecked")
  public synchronized <K, V, U> TableComponents<K, V, U> getTableComponents(final String tableId)
      throws TableNotExistException {
    final Tuple3<Table, TableComponents, Configuration> tableTuple = tables.get(tableId);
    if (tableTuple == null) {
      // cannot access such table.
      throw new TableNotExistException(tableId + " does not exist or this executor did not associate or subscribe it");
    }
    return tableTuple.getSecond();
  }

  /**
   * @return the number of blocks of each Table.
   */
  public synchronized Map<String, Integer> getTableToNumBlocks() {
    final Map<String, Integer> tableIdToNumBlocks = new HashMap<>();

    tables.forEach((tableId, value) -> {
      final TableComponents tableComponents = value.getSecond();

      tableIdToNumBlocks.put(tableId, tableComponents.getBlockStore().getNumBlocks());
    });

    return tableIdToNumBlocks;
  }
}
