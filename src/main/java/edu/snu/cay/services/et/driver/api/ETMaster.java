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

import edu.snu.cay.services.et.configuration.ExecutorConfiguration;
import edu.snu.cay.services.et.configuration.TableConfiguration;
import edu.snu.cay.services.et.driver.impl.AllocatedTable;
import edu.snu.cay.services.et.driver.impl.ETMasterImpl;
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
   * @return a list of {@link AllocatedExecutor}s
   */
  List<AllocatedExecutor> addExecutors(int num, ExecutorConfiguration executorConf);

  /**
   * Creates a table using the given table configuration.
   * It evenly partitions table blocks to {@code initialAssociators}.
   * So it requires at least one executor to be associated with the table.
   * @param tableConf a configuration of table (See {@link TableConfiguration})
   * @param initialAssociators a list of executors to be associated to a table
   * @return a {@link AllocatedTable}, master-side representation of Table allocated in the associated executors
   */
  AllocatedTable createTable(TableConfiguration tableConf, List<AllocatedExecutor> initialAssociators);
}
