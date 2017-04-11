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
package edu.snu.cay.services.et.examples.addinteger;

import edu.snu.cay.common.param.Parameters;
import edu.snu.cay.services.et.configuration.ETDriverConfiguration;
import edu.snu.cay.services.et.configuration.metric.MetricServiceDriverConf;
import edu.snu.cay.services.et.driver.impl.LoggingMetricReceiver;
import edu.snu.cay.services.et.examples.addinteger.parameters.*;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.io.network.naming.LocalNameResolverConfiguration;
import org.apache.reef.io.network.naming.NameServerConfiguration;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.tang.*;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.wake.IdentifierFactory;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Add integer example for demonstrating the capabilities of ET as a parameter server.
 */
public final class AddIntegerET {
  private static final Logger LOG = Logger.getLogger(AddIntegerET.class.getName());

  private static final String DRIVER_IDENTIFIER = "AddInteger";

  /**
   * Should not be instantiated.
   */
  private AddIntegerET() {
  }

  /**
   * Runs AddIntegerET example app.
   * @throws InjectionException when fail to inject DriverLauncher
   */
  public static LauncherStatus runAddIntegerET(final Configuration commandLineConf) throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector(commandLineConf);
    final int numTotalEvals = injector.getNamedInstance(NumWorkers.class) + injector.getNamedInstance(NumServers.class);

    final Configuration runtimeConfiguration = LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, numTotalEvals)
        .build();
    final Configuration driverConfiguration = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(AddIntegerETDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, DRIVER_IDENTIFIER)
        .set(DriverConfiguration.ON_DRIVER_STARTED, AddIntegerETDriver.StartHandler.class)
        .build();
    final Configuration etMasterConfiguration = ETDriverConfiguration.CONF.build();
    final Configuration nameServerConfiguration = NameServerConfiguration.CONF.build();
    final Configuration nameClientConfiguration = LocalNameResolverConfiguration.CONF.build();
    final Configuration implConfiguration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
        .build();
    final Configuration metricServiceConf = MetricServiceDriverConf.CONF
        .set(MetricServiceDriverConf.METRIC_RECEIVER_IMPL, LoggingMetricReceiver.class)
        .build();

    return DriverLauncher.getLauncher(runtimeConfiguration)
        .run(Configurations.merge(driverConfiguration,
        etMasterConfiguration, nameServerConfiguration, nameClientConfiguration,
        implConfiguration, metricServiceConf, commandLineConf), injector.getNamedInstance(Parameters.Timeout.class));
  }

  private static Configuration parseCommandLine(final String[] args) throws IOException, InjectionException {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    new CommandLine(cb)
        .registerShortNameOfClass(Parameters.Timeout.class)
        .registerShortNameOfClass(NumUpdates.class)
        .registerShortNameOfClass(NumKeys.class)
        .registerShortNameOfClass(StartKey.class)
        .registerShortNameOfClass(DeltaValue.class)
        .registerShortNameOfClass(UpdateCoefficient.class)
        .registerShortNameOfClass(NumWorkers.class)
        .registerShortNameOfClass(NumServers.class)
        .registerShortNameOfClass(MetricFlushPeriodMs.class)
        .processCommandLine(args);

    final Configuration clConf = cb.build();
    validateArgs(clConf);

    return clConf;
  }

  private static void validateArgs(final Configuration commandLineConf) throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector(commandLineConf);
    final int numUpdates = injector.getNamedInstance(NumUpdates.class);
    final int numKeys = injector.getNamedInstance(NumKeys.class);

    if (numUpdates % numKeys != 0) {
      throw new IllegalArgumentException(
          String.format("numUpdates %d must be divisible by numKeys %d",
              numUpdates, numKeys));
    }
  }

  public static void main(final String[] args) throws InjectionException, IOException {
    final Configuration clConf = parseCommandLine(args);
    final LauncherStatus status = runAddIntegerET(clConf);
    LOG.log(Level.INFO, "ET job completed: {0}", status);
  }
}
