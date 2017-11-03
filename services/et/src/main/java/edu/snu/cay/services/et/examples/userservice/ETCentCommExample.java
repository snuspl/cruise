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
package edu.snu.cay.services.et.examples.userservice;

import edu.snu.cay.common.centcomm.CentCommConf;
import edu.snu.cay.common.param.Parameters;
import edu.snu.cay.services.et.configuration.ETDriverConfiguration;
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

import javax.inject.Inject;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Central Communication Service Example.
 */
public final class ETCentCommExample {
  private static final Logger LOG = Logger.getLogger(ETCentCommExample.class.getName());
  static final String CENT_COMM_CLIENT_ID = "CENT_COMM_CLIENT_ID";

  @Inject
  private ETCentCommExample() {
  }

  public static void main(final String[] args) throws IOException, InjectionException {
    final Configuration clConf = parseCommandLine(args);
    final LauncherStatus status = runCentCommExample(clConf);
    LOG.log(Level.INFO, "ET job completed: {0}", status);
  }

  public static LauncherStatus runCentCommExample(final Configuration commandLineConf) throws InjectionException {
    final Injector commandLineInjector = Tang.Factory.getTang().newInjector(commandLineConf);

    final boolean onLocal = commandLineInjector.getNamedInstance(Parameters.OnLocal.class);
    final int splits = commandLineInjector.getNamedInstance(Parameters.Splits.class);
    final Configuration runTimeConf = onLocal ?
        getLocalRuntimeConfiguration(splits) :
        getYarnRuntimeConfiguration();

    final Configuration driverConf = getDriverConfiguration(commandLineConf);
    final int timeout = commandLineInjector.getNamedInstance(Parameters.Timeout.class);

    return DriverLauncher.getLauncher(runTimeConf).run(driverConf, timeout);
  }

  private static Configuration getDriverConfiguration(final Configuration commandLineConf) {
    final Configuration driverConf = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(ETCentCommExampleDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "ETCentCommExample")
        .set(DriverConfiguration.ON_DRIVER_STARTED, ETCentCommExampleDriver.StartHandler.class)
        .set(DriverConfiguration.ON_TASK_RUNNING, ETCentCommExampleDriver.RunningTaskHandler.class)
        .build();

    final Configuration etMasterConfiguration = ETDriverConfiguration.CONF.build();

    final Configuration centCommConf = CentCommConf.newBuilder()
        .addCentCommClient(CENT_COMM_CLIENT_ID,
            DriverSideMsgHandler.class,
            EvalSideMsgHandler.class)
        .build()
        .getDriverConfiguration();

    final Configuration idFactoryConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
        .build();

    return Configurations.merge(driverConf, commandLineConf, etMasterConfiguration, centCommConf, idFactoryConf,
        NameServerConfiguration.CONF.build(), LocalNameResolverConfiguration.CONF.build());
  }

  private static Configuration parseCommandLine(final String[] args) throws IOException {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    final CommandLine cl = new CommandLine(cb);
    // add all basic parameters
    cl.registerShortNameOfClass(Parameters.OnLocal.class);
    cl.registerShortNameOfClass(Parameters.Splits.class);
    cl.registerShortNameOfClass(Parameters.Timeout.class);

    cl.processCommandLine(args);
    return cb.build();
  }

  private static Configuration getYarnRuntimeConfiguration() {
    return YarnClientConfiguration.CONF.build();
  }

  private static Configuration getLocalRuntimeConfiguration(final int maxNumEvalLocal) {
    return LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, Integer.toString(maxNumEvalLocal))
        .build();
  }
}
