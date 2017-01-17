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
package edu.snu.cay.services.et.driver.api;

import edu.snu.cay.services.et.configuration.ResourceConfiguration;
import edu.snu.cay.services.et.configuration.TableConfiguration;
import edu.snu.cay.services.et.driver.impl.AllocatedTable;
import edu.snu.cay.services.et.driver.impl.ETMasterImpl;
import edu.snu.cay.services.et.driver.impl.RawTable;
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
   * @param resConf resource configuration
   * @return a list of {@link AllocatedExecutor}s
   */
  List<AllocatedExecutor> addExecutors(int num, ResourceConfiguration resConf);

  /**
   * Creates a Table using the given table configuration.
   * @param tableConf a configuration of table (See {@link TableConfiguration})
   * @return a logical representation of Table ({@link RawTable}),
   *   which will be converted to a {@link AllocatedTable}
   *   at the associated executors.
   */
  RawTable createTable(TableConfiguration tableConf);
}
