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

import edu.snu.cay.services.et.common.api.NetworkConnection;
import edu.snu.cay.services.et.configuration.ResourceConfiguration;
import edu.snu.cay.services.et.configuration.TableConfiguration;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.ETMaster;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.util.List;

/**
 * Implementation for {@link ETMaster}.
 */
@DriverSide
public final class ETMasterImpl implements ETMaster {
  private final ExecutorManager executorManager;
  private final TableManager tableManager;

  @Inject
  private ETMasterImpl(final ExecutorManager executorManager,
                       final TableManager tableManager,
                       final NetworkConnection networkConnection,
                       @Parameter(DriverIdentifier.class) final String driverId) {
    this.executorManager = executorManager;
    this.tableManager = tableManager;
    networkConnection.setup(driverId);
  }

  @Override
  public List<AllocatedExecutor> addExecutors(final int num, final ResourceConfiguration resConf) {
    return executorManager.addExecutors(num, resConf);
  }

  @Override
  public AllocatedTable createTable(final TableConfiguration tableConf,
                                    final List<AllocatedExecutor> initialAssociators) {
    try {
      return tableManager.createTable(tableConf, initialAssociators);
    } catch (final InjectionException e) {
      throw new RuntimeException("The given table configuration is incomplete", e);
    }
  }
}
