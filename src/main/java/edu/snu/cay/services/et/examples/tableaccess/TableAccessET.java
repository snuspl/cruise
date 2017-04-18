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
package edu.snu.cay.services.et.examples.tableaccess;

import edu.snu.cay.common.centcomm.CentCommConf;
import edu.snu.cay.common.client.DriverLauncher;
import edu.snu.cay.services.et.configuration.ETDriverConfiguration;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.io.network.naming.LocalNameResolverConfiguration;
import org.apache.reef.io.network.naming.NameServerConfiguration;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.wake.IdentifierFactory;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.cay.services.et.examples.tableaccess.TableAccessETDriver.NUM_EXECUTORS;

/**
 * Client code for table access example.
 */
public final class TableAccessET {
  private static final Logger LOG = Logger.getLogger(TableAccessET.class.getName());

  private static final String DRIVER_IDENTIFIER = "TableAccess";

  // the number of associator + the number of subscriber + driver
  private static final int MAX_NUMBER_OF_EVALUATORS = NUM_EXECUTORS * 2 + 1;
  private static final int JOB_TIMEOUT = 300000; // 300 sec.

  /**
   * Should not be instantiated.
   */
  private TableAccessET() {

  }

  public static void main(final String[] args) throws InjectionException, IOException {
    final LauncherStatus status = runTableAccessET();
    LOG.log(Level.INFO, "ET job completed: {0}", status);
  }

  /**
   * Runs TableAccessET example app.
   * @throws InjectionException when fail to inject DriverLauncher
   */
  public static LauncherStatus runTableAccessET() throws InjectionException {

    final Configuration runtimeConfiguration = LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, MAX_NUMBER_OF_EVALUATORS)
        .build();

    final Configuration driverConfiguration = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(TableAccessETDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, DRIVER_IDENTIFIER)
        .set(DriverConfiguration.ON_DRIVER_STARTED, TableAccessETDriver.StartHandler.class)
        .build();

    final Configuration etMasterConfiguration = ETDriverConfiguration.CONF.build();
    final Configuration nameServerConfiguration = NameServerConfiguration.CONF.build();
    final Configuration nameClientConfiguration = LocalNameResolverConfiguration.CONF.build();
    final Configuration implConfiguration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
        .build();

    final Configuration centCommConfiguration = CentCommConf.newBuilder()
        .addCentCommClient(TableAccessETDriver.CENTCOMM_CLIENT_ID,
            ExecutorSyncManager.class,
            ExecutorSynchronizer.class)
        .build()
        .getDriverConfiguration();

    return DriverLauncher.getLauncher(runtimeConfiguration)
        .run(Configurations.merge(driverConfiguration,
        etMasterConfiguration, nameServerConfiguration, nameClientConfiguration,
        implConfiguration, centCommConfiguration), JOB_TIMEOUT);
  }
}
