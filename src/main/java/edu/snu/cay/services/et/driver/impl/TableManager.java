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

import edu.snu.cay.services.et.configuration.TableConfiguration;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A manager class of Tables, which creates and manages materialized tables.
 * Table init is done through two stages: {@link RawTable}, {@link MaterializedTable}.
 *
 * At first, by calling {@link #createTable(TableConfiguration)}, users can obtain {@link RawTable}
 * through which users can make containers subscribe or associate to the table.
 *
 * The table can become a {@link MaterializedTable} by calling {@link RawTable#materialize()},
 * which will be actually allocated to the containers that subscribe or associate with the table.
 * Once materialized, {@link MaterializedTable} will be registered into {@link TableManager}
 * using {@link #onMaterializedTable(MaterializedTable)}.
 * Containers can associate or subscribe to containers in any state of table.
 */
@Private
@DriverSide
final class TableManager {
  private final MigrationManager migrationManager;
  private final TableInitializer tableInitializer;

  private final Map<String, MaterializedTable> materializedTableMap = new ConcurrentHashMap<>();

  @Inject
  private TableManager(final MigrationManager migrationManager,
                       final TableInitializer tableInitializer) throws InjectionException {
    this.migrationManager = migrationManager;
    this.tableInitializer = tableInitializer;
  }

  /**
   * Creates a {@link RawTable} based on the given table configuration.
   * @param tableConf a configuration of table (See {@link edu.snu.cay.services.et.configuration.TableConfiguration})
   * @return a {@link RawTable}, which is for associating and allocating table to containers
   * @throws InjectionException when the given configuration is incomplete
   */
  RawTable createTable(final TableConfiguration tableConf) throws InjectionException {
    return new RawTable(tableConf, this, migrationManager, tableInitializer);
  }

  /**
   * Registers a materialized table to be accessed by {@link #getMaterializedTable(String)}.
   * @param materializedTable an allocated table
   */
  void onMaterializedTable(final MaterializedTable materializedTable) {
    materializedTableMap.putIfAbsent(materializedTable.getTableConfiguration().getId(), materializedTable);
  }

  /**
   * @return {@link MaterializedTable} whose id is {@code tableId}, or
   *         {@code null} if it has no table for the id
   */
  MaterializedTable getMaterializedTable(final String tableId) {
    return materializedTableMap.get(tableId);
  }
}
