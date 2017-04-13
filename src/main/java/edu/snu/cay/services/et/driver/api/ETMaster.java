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
package edu.snu.cay.services.et.driver.api;

import edu.snu.cay.services.et.common.util.concurrent.ListenableFuture;
import edu.snu.cay.services.et.configuration.ExecutorConfiguration;
import edu.snu.cay.services.et.configuration.TableConfiguration;
import edu.snu.cay.services.et.driver.impl.AllocatedTable;
import edu.snu.cay.services.et.driver.impl.ETMasterImpl;
import edu.snu.cay.services.et.exceptions.ExecutorNotExistException;
import edu.snu.cay.services.et.exceptions.TableNotExistException;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.List;

/**
 * Driver-side API.
 */
@DriverSide
@DefaultImplementation(ETMasterImpl.class)
public interface ETMaster {

  /**
   * Allocates new {@code num} executors of the equal resource specification.
   * @param num the number of executors
   * @param executorConf executor configuration
   * @return a {@link ListenableFuture} for a list of {@link AllocatedExecutor}s
   */
  ListenableFuture<List<AllocatedExecutor>> addExecutors(int num, ExecutorConfiguration executorConf);

  /**
   * Creates a table using the given table configuration.
   * It evenly partitions table blocks to {@code initialAssociators}.
   * So it requires at least one executor to be associated with the table.
   * @param tableConf a configuration of table (See {@link TableConfiguration})
   * @param initialAssociators a list of executors to be associated to a table
   * @return a {@link ListenableFuture} for a {@link AllocatedTable},
   *         which is a master-side representation of Table allocated in the associated executors
   */
  ListenableFuture<AllocatedTable> createTable(TableConfiguration tableConf,
                                               List<AllocatedExecutor> initialAssociators);

  /**
   * Return an existing executor whose id is {@code executorId}.
   * @param executorId an executor id
   * @return {@link AllocatedExecutor} whose id is {@code executorId}
   * @throws ExecutorNotExistException when there's no existing executor with {@code executorId}
   */
  AllocatedExecutor getExecutor(String executorId) throws ExecutorNotExistException;

  /**
   * Return a existing table whose id is {@code tableId}.
   * @param tableId a table id
   * @return {@link AllocatedTable} whose id is {@code tableId}
   * @throws TableNotExistException when there's no existing table with {@code tableId}
   */
  AllocatedTable getTable(String tableId) throws TableNotExistException;
}
