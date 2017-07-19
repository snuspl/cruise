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
package edu.snu.cay.pregel;


import edu.snu.cay.common.centcomm.CentCommConf;
import edu.snu.cay.services.et.configuration.ETDriverConfiguration;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.io.network.naming.LocalNameResolverConfiguration;
import org.apache.reef.io.network.naming.NameServerConfiguration;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.ConfigurationBuilder;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.wake.IdentifierFactory;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Launcher for Pregel application.
 */
@ClientSide
public final class PregelLauncher {

  private static final Logger LOG = Logger.getLogger(PregelLauncher.class.getName());

  private static final int JOB_TIMEOUT = 300000;
  private static final int MAX_NUMBER_OF_EVALUATORS = 5;
  private static final String DRIVER_IDENTIFIER = "Pregel";

  /**
   * Should not be instantiated.
   */
  private PregelLauncher() {
  }

  public static void main(final String[] args) throws InjectionException, IOException {
    final Configuration clConf = parseCommandLine(args);
    final String tableInputPath = Tang.Factory.getTang().newInjector(clConf)
        .getNamedInstance(InputPath.class);

    final LauncherStatus status = launch(tableInputPath);
    LOG.log(Level.INFO, "Pregel job completed: {0}", status);
  }

  private static Configuration parseCommandLine(final String[] args) throws IOException {
    final ConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    final CommandLine cl = new CommandLine(cb);
    cl.registerShortNameOfClass(InputPath.class).processCommandLine(args);
    return cb.build();
  }

  public static LauncherStatus launch(final String inputPath) throws InjectionException {

    final Configuration runtimeConfiguration = LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, MAX_NUMBER_OF_EVALUATORS)
        .build();

    final Configuration driverConfiguration = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(PregelDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, DRIVER_IDENTIFIER)
        .set(DriverConfiguration.ON_DRIVER_STARTED, PregelDriver.StartHandler.class)
        .build();

    final Configuration etMasterConfiguration = ETDriverConfiguration.CONF.build();
    final Configuration nameServerConfiguration = NameServerConfiguration.CONF.build();
    final Configuration nameClientConfiguration = LocalNameResolverConfiguration.CONF.build();
    final Configuration idFactoryConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
        .build();

    final Configuration inputPathConfiguration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(InputPath.class, inputPath)
        .build();

    final Configuration centCommConfiguration = CentCommConf.newBuilder()
        .addCentCommClient(PregelDriver.CENTCOMM_CLIENT_ID,
            PregelMaster.MasterMsgHandler.class,
            WorkerMsgManager.class)
        .build()
        .getDriverConfiguration();

    return DriverLauncher.getLauncher(runtimeConfiguration)
        .run(Configurations.merge(driverConfiguration, centCommConfiguration, inputPathConfiguration,
            idFactoryConf, nameClientConfiguration, nameServerConfiguration, etMasterConfiguration), JOB_TIMEOUT);
  }

  @NamedParameter(doc = "Path of a input file",
      short_name = "input_path")
  final class InputPath implements Name<String> {

    // should not be instantiated
    private InputPath() {

    }
  }
}
