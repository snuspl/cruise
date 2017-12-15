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
package edu.snu.spl.cruise.services.et.examples.checkpoint;

import edu.snu.spl.cruise.common.param.Parameters;
import edu.snu.spl.cruise.services.et.configuration.ETDriverConfiguration;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.io.network.naming.LocalNameResolverConfiguration;
import org.apache.reef.io.network.naming.NameServerConfiguration;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.*;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.wake.IdentifierFactory;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Client code for table checkpoint example.
 */
public final class CheckpointET {
  private static final Logger LOG = Logger.getLogger(CheckpointET.class.getName());

  private static final String DRIVER_IDENTIFIER = "Checkpoint";
  private static final int MAX_NUMBER_OF_EVALUATORS = CheckpointETDriver.NUM_EXECUTORS * 2;
  private static final int JOB_TIMEOUT = 30000; // 30 sec.

  /**
   * Should not be instantiated.
   */
  private CheckpointET() {

  }

  public static void main(final String[] args) throws InjectionException, IOException {
    final LauncherStatus status = runCheckpointET(args);
    LOG.log(Level.INFO, "ET job completed: {0}", status);
  }

  private static Configuration parseCommandLine(final String[] args) throws IOException {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    final CommandLine cl = new CommandLine(cb);
    // add all basic parameters
    cl.registerShortNameOfClass(Parameters.OnLocal.class);

    cl.processCommandLine(args);
    return cb.build();
  }

  /**
   * Runs CheckpointET example app.
   * @throws InjectionException when fail to inject DriverLauncher
   */
  public static LauncherStatus runCheckpointET(final String[] args) throws InjectionException, IOException {
    final Configuration commandLineConf = parseCommandLine(args);
    final Injector commandLineInjector = Tang.Factory.getTang().newInjector(commandLineConf);

    final boolean onLocal = commandLineInjector.getNamedInstance(Parameters.OnLocal.class);

    final Configuration runtimeConfiguration = onLocal ?
        LocalRuntimeConfiguration.CONF
            .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, MAX_NUMBER_OF_EVALUATORS)
            .build() :
        YarnClientConfiguration.CONF.build();
    final Configuration driverConfiguration = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(CheckpointETDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, DRIVER_IDENTIFIER)
        .set(DriverConfiguration.ON_DRIVER_STARTED, CheckpointETDriver.StartHandler.class)
        .build();
    final Configuration etMasterConfiguration = ETDriverConfiguration.CONF.build();
    final Configuration nameServerConfiguration = NameServerConfiguration.CONF.build();
    final Configuration nameClientConfiguration = LocalNameResolverConfiguration.CONF.build();
    final Configuration implConfiguration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
        .build();

    return DriverLauncher.getLauncher(runtimeConfiguration)
        .run(Configurations.merge(driverConfiguration,
        etMasterConfiguration, nameServerConfiguration, nameClientConfiguration,
        implConfiguration), JOB_TIMEOUT);
  }
}
