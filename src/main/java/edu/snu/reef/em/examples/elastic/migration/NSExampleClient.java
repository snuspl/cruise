/**
 * Copyright (C) 2014 Seoul National University
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

package edu.snu.reef.em.examples.elastic.migration;

import edu.snu.reef.elastic.memory.driver.ContextMsgSender;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.io.network.group.impl.driver.GroupCommService;
import org.apache.reef.io.network.naming.NameServerConfiguration;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.EnvironmentUtils;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Client for Network Service Example
 */
public final class NSExampleClient {

  private static final Logger LOG = Logger.getLogger(NSExampleClient.class.getName());
  private static final int TIMEOUT = 100000;
  private static final int MAX_NUM_OF_EVALUATORS = 16;

  public static Configuration getDriverConfiguration() {
    Configuration driverConfiguration = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(NSExampleDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "NSExampleDriver")
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, NSExampleDriver.EvaluatorAllocatedHandler.class)
        .set(DriverConfiguration.ON_DRIVER_STARTED, NSExampleDriver.DriverStartHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, NSExampleDriver.ActiveContextHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_CLOSED, NSExampleDriver.ContextCloseHandler.class)
        .set(DriverConfiguration.ON_DRIVER_STOP, NSExampleDriver.DriverStopHandler.class)
        .set(DriverConfiguration.ON_TASK_MESSAGE, NSExampleDriver.TaskMessageHandler.class)
        .build();

    return Tang.Factory.getTang()
        .newConfigurationBuilder(driverConfiguration,
                                 ContextMsgSender.getConfiguration(),
                                 GroupCommService.getConfiguration(),
                                 NameServerConfiguration.CONF.build())
        .build();
  }

  public static LauncherStatus runNSExample(final Configuration runtimeConf, final int timeOut)
      throws InjectionException {
    final Configuration driverConf = getDriverConfiguration();

    return DriverLauncher.getLauncher(runtimeConf).run(driverConf, timeOut);
  }

  public static void main(final String[] args) throws InjectionException{
    final Configuration localRuntimeConfiguration = LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, MAX_NUM_OF_EVALUATORS)
        .build();
    final LauncherStatus status = runNSExample(localRuntimeConfiguration, TIMEOUT);
    LOG.log(Level.INFO, "REEF job completed: {0}", status);
  }
}

