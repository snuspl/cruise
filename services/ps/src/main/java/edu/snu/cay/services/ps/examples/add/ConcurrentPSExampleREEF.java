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
package edu.snu.cay.services.ps.examples.add;

import edu.snu.cay.services.ps.ParameterServerConfigurationBuilder;
import edu.snu.cay.services.ps.driver.impl.ConcurrentParameterServerManager;
import edu.snu.cay.services.ps.examples.add.parameters.NumKeys;
import edu.snu.cay.services.ps.examples.add.parameters.NumUpdates;
import edu.snu.cay.services.ps.examples.add.parameters.NumWorkers;
import edu.snu.cay.services.ps.examples.add.parameters.JobTimeout;
import edu.snu.cay.services.ps.examples.add.parameters.StartKey;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.util.EnvironmentUtils;

import javax.inject.Inject;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ParameterServer Example for the Concurrent (single-node) PS.
 */
public final class ConcurrentPSExampleREEF {
  private static final Logger LOG = Logger.getLogger(ConcurrentPSExampleREEF.class.getName());

  private final long timeout;
  private final int numWorkers;
  private final int numUpdates;
  private final int startKey;
  private final int numKeys;

  @Inject
  private ConcurrentPSExampleREEF(@Parameter(JobTimeout.class) final long timeout,
                                  @Parameter(NumWorkers.class) final int numWorkers,
                                  @Parameter(NumUpdates.class) final int numUpdates,
                                  @Parameter(StartKey.class) final int startKey,
                                  @Parameter(NumKeys.class) final int numKeys) {
    this.timeout = timeout;
    this.numWorkers = numWorkers;
    this.numUpdates = numUpdates;
    this.startKey = startKey;
    this.numKeys = numKeys;
  }

  private Configuration getDriverConf() {
    final Configuration driverConf = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES,
            EnvironmentUtils.getClassLocation(PSExampleDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "ConcurrentPSExample")
        .set(DriverConfiguration.ON_DRIVER_STARTED,
            PSExampleDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED,
            PSExampleDriver.EvaluatorAllocatedHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE,
            PSExampleDriver.ActiveContextHandler.class)
        .set(DriverConfiguration.ON_TASK_RUNNING,
            PSExampleDriver.RunningTaskHandler.class)
        .set(DriverConfiguration.ON_TASK_COMPLETED,
            PSExampleDriver.CompletedTaskHandler.class)
        .build();

    final Configuration parametersConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(NumWorkers.class, Integer.toString(numWorkers))
        .bindNamedParameter(NumUpdates.class, Integer.toString(numUpdates))
        .bindNamedParameter(StartKey.class, Integer.toString(startKey))
        .bindNamedParameter(NumKeys.class, Integer.toString(numKeys))
        .build();

    final Configuration psConf = new ParameterServerConfigurationBuilder()
        .setManagerClass(ConcurrentParameterServerManager.class)
        .setUpdaterClass(AddUpdater.class)
        .setKeyCodecClass(IntegerCodec.class)
        .setPreValueCodecClass(IntegerCodec.class)
        .setValueCodecClass(IntegerCodec.class)
        .build();

    return Configurations.merge(driverConf, parametersConf, psConf);
  }

  private Configuration getRuntimeConfiguration() {
    return getLocalRuntimeConfiguration();
  }

  private Configuration getLocalRuntimeConfiguration() {
    return LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, numWorkers + 1)
        .build();
  }

  private LauncherStatus run() throws InjectionException {
    return DriverLauncher.getLauncher(getRuntimeConfiguration()).run(getDriverConf(), timeout);
  }

  private static ConcurrentPSExampleREEF parseCommandLine(final String[] args) throws IOException, InjectionException {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    final CommandLine cl = new CommandLine(cb);
    cl.registerShortNameOfClass(JobTimeout.class);
    cl.registerShortNameOfClass(NumWorkers.class);
    cl.registerShortNameOfClass(NumUpdates.class);
    cl.registerShortNameOfClass(StartKey.class);
    cl.registerShortNameOfClass(NumKeys.class);

    cl.processCommandLine(args);

    return Tang.Factory.getTang().newInjector(cb.build()).getInstance(ConcurrentPSExampleREEF.class);
  }

  public static void main(final String[] args) {
    LauncherStatus status;
    try {
      final ConcurrentPSExampleREEF concurrentPSExampleREEF = parseCommandLine(args);
      status = concurrentPSExampleREEF.run();
    } catch (final Exception e) {
      LOG.log(Level.SEVERE, "Fatal exception occurred: {0}", e);
      status = LauncherStatus.failed(e);
    }
    LOG.log(Level.INFO, "REEF job completed: {0}", status);
  }
}
