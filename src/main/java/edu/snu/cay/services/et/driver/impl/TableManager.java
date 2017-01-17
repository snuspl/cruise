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
 * A manager class of Tables, which creates and manages allocated tables.
 * Table init is done through two stages: {@link RawTable}, {@link AllocatedTable}.
 *
 * At first, by calling {@link #createTable(TableConfiguration)}, users can obtain {@link RawTable}
 * through which users can make executors subscribe or associate to the table.
 *
 * The table can become a {@link AllocatedTable} by calling {@link RawTable#allocate()},
 * which will be actually allocated to the executors that associate with the table.
 * Once allocated, {@link AllocatedTable} will be registered into {@link TableManager}
 * using {@link #onAllocatedTable(AllocatedTable)}.
 * Executors can associate or subscribe to tables in any state.
 */
@Private
@DriverSide
final class TableManager {
  private final MigrationManager migrationManager;
  private final TableInitializer tableInitializer;

  private final Map<String, AllocatedTable> allocatedTableMap = new ConcurrentHashMap<>();

  @Inject
  private TableManager(final MigrationManager migrationManager,
                       final TableInitializer tableInitializer) throws InjectionException {
    this.migrationManager = migrationManager;
    this.tableInitializer = tableInitializer;
  }

  /**
   * Creates a {@link RawTable} based on the given table configuration.
   * @param tableConf a configuration of table (See {@link edu.snu.cay.services.et.configuration.TableConfiguration})
   * @return a {@link RawTable}, which is for associating and allocating table to executors
   * @throws InjectionException when the given configuration is incomplete
   */
  RawTable createTable(final TableConfiguration tableConf) throws InjectionException {
    return new RawTable(tableConf, this, migrationManager, tableInitializer);
  }

  /**
   * Registers an allocated table to be accessed by {@link #getAllocatedTable(String)}.
   * @param allocatedTable an allocated table
   */
  void onAllocatedTable(final AllocatedTable allocatedTable) {
    allocatedTableMap.putIfAbsent(allocatedTable.getTableConfiguration().getId(), allocatedTable);
  }

  /**
   * @return {@link AllocatedTable} whose id is {@code tableId}, or
   *         {@code null} if it has no table for the id
   */
  AllocatedTable getAllocatedTable(final String tableId) {
    return allocatedTableMap.get(tableId);
  }
}
