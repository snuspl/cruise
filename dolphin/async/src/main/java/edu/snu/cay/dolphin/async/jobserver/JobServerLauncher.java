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
package edu.snu.cay.dolphin.async.jobserver;

import edu.snu.cay.common.client.DriverLauncher;
import edu.snu.cay.services.et.configuration.ETDriverConfiguration;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.io.network.naming.LocalNameResolverConfiguration;
import org.apache.reef.io.network.naming.NameServerConfiguration;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.webserver.HttpHandlerConfiguration;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Main entry point for launching a Job Server on ET application.
 */
@ClientSide
public final class JobServerLauncher {
  private static final Logger LOG = Logger.getLogger(JobServerLauncher.class.getName());
  private static final int MAX_NUMBER_OF_EVALUATORS = 5;
  private static final String JOB_NAME = "JOB_SERVER";
  private static final int TIME_OUT = 10000000;

  /**
   * Should not be instantiated.
   */
  private JobServerLauncher() {
  }

  public static LauncherStatus launch() {
    LauncherStatus status;

    try {

      // runtime configuration
      final boolean onLocal = true;
      final Configuration runTimeConf = onLocal ? getLocalRuntimeConfiguration() : getYarnRuntimeConfiguration();

      // driver configuration
      final Configuration driverConf = getDriverConfiguration(JOB_NAME);
      status = DriverLauncher.getLauncher(runTimeConf).run(driverConf, TIME_OUT);

    } catch (final Exception e) {
      status = LauncherStatus.failed(e);
      // This log is for giving more detailed info about failure, which status object does not show
      LOG.log(Level.WARNING, "Exception occurred", e);
    }

    LOG.log(Level.INFO, "REEF job completed: {0}", status);
    return status;
  }

  private static Configuration getYarnRuntimeConfiguration() {
    return YarnClientConfiguration.CONF
        .build();
  }

  private static Configuration getLocalRuntimeConfiguration() {
    return LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, Integer.toString(MAX_NUMBER_OF_EVALUATORS))
        .build();
  }

  private static Configuration getDriverConfiguration(final String jobName) {
    final Configuration driverConf = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(JobServerDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, jobName)
        .set(DriverConfiguration.ON_DRIVER_STARTED, JobServerDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_FAILED, JobServerDriver.FailedEvaluatorHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_FAILED, JobServerDriver.FailedContextHandler.class)
        .set(DriverConfiguration.ON_TASK_FAILED, JobServerDriver.FailedTaskHandler.class)
        .build();

    final Configuration etMasterConfiguration = ETDriverConfiguration.CONF.build();

    final Configuration httpConf = HttpHandlerConfiguration.CONF
        .set(HttpHandlerConfiguration.HTTP_HANDLERS, JobServerHttpHandler.class)
        .build();

    return Configurations.merge(driverConf, etMasterConfiguration, httpConf, getNCSConfiguration());
  }

  private static Configuration getNCSConfiguration() {
    final Configuration nameServerConfiguration = NameServerConfiguration.CONF.build();
    final Configuration nameClientConfiguration = LocalNameResolverConfiguration.CONF.build();
    final Configuration idFactoryImplConfiguration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
        .build();

    return Configurations.merge(nameServerConfiguration, nameClientConfiguration, idFactoryImplConfiguration);
  }

  public static void main(final String[] args) {
    final LauncherStatus status = launch();
    LOG.log(Level.INFO, "ET job completed: {0}", status);
  }

}
