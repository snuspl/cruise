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
package edu.snu.cay.common.dataloader.examples;

import edu.snu.cay.common.param.Parameters;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.io.network.naming.LocalNameResolverConfiguration;
import org.apache.reef.io.network.naming.NameServerConfiguration;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.*;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.util.EnvironmentUtils;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Client for the data loading app.
 */
@ClientSide
public final class LineCountingClient {
  private static final Logger LOG = Logger.getLogger(LineCountingClient.class.getName());
  private static final String DRIVER_ID = "LineCounting";

  @Inject
  private LineCountingClient() {
  }

  public static void main(final String[] args) {
    LauncherStatus status;
    try {
      status = run(args);
    } catch (final Exception e) {
      LOG.log(Level.SEVERE, "Fatal exception occurred.", e);
      status = LauncherStatus.failed(e);
    }
    LOG.log(Level.INFO, "REEF job completed: {0}", status);
  }

  private static LauncherStatus run(final String[] args) throws IOException, InjectionException {
    final Configuration commandLineConf = parseCommandLine(args);
    final Injector commandLineInjector = Tang.Factory.getTang().newInjector(commandLineConf);

    final boolean onLocal = commandLineInjector.getNamedInstance(Parameters.OnLocal.class);
    final String inputDir = processInputDir(commandLineInjector.getNamedInstance(Parameters.InputDir.class), onLocal);
    final int splits = commandLineInjector.getNamedInstance(Parameters.Splits.class);

    final Configuration paramConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(Parameters.OnLocal.class, Boolean.toString(onLocal))
        .bindNamedParameter(Parameters.InputDir.class, inputDir)
        .bindNamedParameter(Parameters.Splits.class, Integer.toString(splits))
        .build();

    final Configuration runTimeConf = onLocal ?
        getLocalRuntimeConfiguration(splits) :
        getYarnRuntimeConfiguration();

    final Configuration driverConf = getDriverConfiguration(paramConf);
    final int timeout = commandLineInjector.getNamedInstance(Parameters.Timeout.class);

    return DriverLauncher.getLauncher(runTimeConf).run(driverConf, timeout);
  }

  private static String processInputDir(final String inputDir, final boolean onLocal) {
    if (!onLocal) {
      return inputDir;
    }
    final File inputFile = new File(inputDir);
    return "file:///" + inputFile.getAbsolutePath();
  }

  private static Configuration getDriverConfiguration(final Configuration paramConf) {
    final Configuration driverConf = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(LineCountingDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, DRIVER_ID)
        .set(DriverConfiguration.ON_DRIVER_STARTED, LineCountingDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, LineCountingDriver.EvaluatorAllocatedHandler.class)
        .set(DriverConfiguration.ON_TASK_COMPLETED, LineCountingDriver.TaskCompletedHandler.class)
        .build();

    return Configurations.merge(driverConf, paramConf,
        NameServerConfiguration.CONF.build(), LocalNameResolverConfiguration.CONF.build());
  }

  private static Configuration parseCommandLine(final String[] args) throws IOException {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    final CommandLine cl = new CommandLine(cb);
    // add all basic parameters
    cl.registerShortNameOfClass(Parameters.OnLocal.class);
    cl.registerShortNameOfClass(Parameters.InputDir.class);
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
