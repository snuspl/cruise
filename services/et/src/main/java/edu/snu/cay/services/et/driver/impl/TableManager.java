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

import edu.snu.cay.services.et.common.util.concurrent.ListenableFuture;
import edu.snu.cay.services.et.common.util.concurrent.ResultFuture;
import edu.snu.cay.services.et.configuration.TableConfiguration;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.AllocatedTable;
import edu.snu.cay.services.et.exceptions.ChkpNotExistException;
import edu.snu.cay.services.et.exceptions.TableNotExistException;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * A manager class of Tables, which creates and manages allocated tables.
 * By calling {@link #createTable(TableConfiguration, List)},
 * users can get an {@link AllocatedTable} that is partitioned into associated executors.
 * More executors can associate with or subscribe to {@link AllocatedTable}.
 */
@Private
@DriverSide
final class TableManager {
  private final Injector baseTableInjector;

  private final ChkpManagerMaster chkpManagerMaster;
  private final Map<String, AllocatedTable> allocatedTableMap = new ConcurrentHashMap<>();

  @Inject
  private TableManager(final MigrationManager migrationManager,
                       final SubscriptionManager subscriptionManager,
                       final TableControlAgent tableControlAgent,
                       final ChkpManagerMaster chkpManagerMaster) throws InjectionException {
    this.baseTableInjector = Tang.Factory.getTang().newInjector();
    baseTableInjector.bindVolatileInstance(MigrationManager.class, migrationManager);
    baseTableInjector.bindVolatileInstance(SubscriptionManager.class, subscriptionManager);
    baseTableInjector.bindVolatileInstance(TableControlAgent.class, tableControlAgent);
    baseTableInjector.bindVolatileInstance(ChkpManagerMaster.class, chkpManagerMaster);
    baseTableInjector.bindVolatileInstance(TableManager.class, this);

    this.chkpManagerMaster = chkpManagerMaster;

    // MigrationManager and TableControlAgent should be instantiated although they are not actually accessed.
    // This is intentional. Otherwise MigrationManager and TableControlAgent are created per Table, which we want
    // to keep singleton.
  }

  /**
   * Creates a table based on the given table configuration.
   * @param tableConf a configuration of table (See {@link edu.snu.cay.services.et.configuration.TableConfiguration})
   * @return an {@link AllocatedTable}, which represents table in driver-side
   * @throws InjectionException when the given configuration is incomplete
   */
  synchronized ListenableFuture<AllocatedTable> createTable(final TableConfiguration tableConf,
                                                            final List<AllocatedExecutor> initialAssociators)
      throws InjectionException {
    if (initialAssociators.isEmpty()) {
      throw new RuntimeException("Table requires at least one associator");
    }

    final String tableId = tableConf.getId();
    if (allocatedTableMap.containsKey(tableId)) {
      throw new RuntimeException(String.format("Table %s already exists", tableId));
    }

    final Injector tableInjector = baseTableInjector.forkInjector(tableConf.getConfiguration());
    final AllocatedTableImpl allocatedTableImpl = tableInjector.getInstance(AllocatedTableImpl.class);

    final ResultFuture<AllocatedTable> resultFuture = new ResultFuture<>();

    allocatedTableImpl.init(tableConf, initialAssociators)
        .addListener(o -> resultFuture.onCompleted(allocatedTableImpl));

    allocatedTableMap.put(tableId, allocatedTableImpl);
    return resultFuture;
  }

  /**
   * Creates a table from a checkpoint.
   * It creates a new empty table and loads check-pointed blocks into the table.
   * @param checkpointId a checkpoint Id
   * @return an {@link AllocatedTable}, which represents table in driver-side
   * @throws ChkpNotExistException
   */
  synchronized ListenableFuture<AllocatedTable> createTable(final String checkpointId,
                                                            final List<AllocatedExecutor> initialAssociators)
      throws ChkpNotExistException {
    if (initialAssociators.isEmpty()) {
      throw new RuntimeException("Table requires at least one associator");
    }

    final TableConfiguration tableConf = chkpManagerMaster.getTableConf(checkpointId);
    final String tableId = tableConf.getId();
    if (allocatedTableMap.containsKey(tableId)) {
      throw new RuntimeException(String.format("Table %s already exists", tableId));
    }

    final Injector tableInjector = baseTableInjector.forkInjector(tableConf.getConfiguration());
    final AllocatedTable allocatedTable;
    try {
      allocatedTable = tableInjector.getInstance(AllocatedTable.class);
    } catch (InjectionException e) {
      throw new RuntimeException(e);
    }

    // load checkpointed blocks into the table
    final ResultFuture<AllocatedTable> resultFuture = new ResultFuture<>();
    allocatedTable.init(tableConf, initialAssociators)
        .addListener(o -> {
          try {
            chkpManagerMaster.load(checkpointId).get();
            resultFuture.onCompleted(allocatedTable);
          } catch (ChkpNotExistException | InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
          }
        });

    allocatedTableMap.put(tableId, allocatedTable);
    return resultFuture;
  }

  /**
   * Removes a table entry from {@link #allocatedTableMap}.
   * It should be called only by {@link AllocatedTable#drop()}.
   * @param tableId an identifier of a table
   */
  void onDropTable(final String tableId) {
    if (allocatedTableMap.remove(tableId) == null) {
      throw new RuntimeException(String.format("Table %s does not exist.", tableId));
    }
  }

  /**
   * @param tableId an identifier of a table
   * @return {@link AllocatedTable} whose id is {@code tableId}
   * @throws TableNotExistException
   */
  AllocatedTable getAllocatedTable(final String tableId) throws TableNotExistException {
    final AllocatedTable table = allocatedTableMap.get(tableId);
    if (table == null) {
      throw new TableNotExistException(tableId + " does not exist.");
    }
    return table;
  }
}
