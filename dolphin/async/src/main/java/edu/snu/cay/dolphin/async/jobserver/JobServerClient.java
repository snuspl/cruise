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

import edu.snu.cay.common.param.Parameters.*;
import edu.snu.cay.dolphin.async.network.NetworkConfProvider;
import edu.snu.cay.services.et.configuration.ETDriverConfiguration;
import edu.snu.cay.services.et.driver.impl.LoggingMetricReceiver;
import edu.snu.cay.services.et.metric.configuration.MetricServiceDriverConf;
import edu.snu.cay.utils.ConfigurationUtils;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.driver.parameters.DriverIdleSources;
import org.apache.reef.io.network.naming.LocalNameResolverConfiguration;
import org.apache.reef.io.network.naming.NameServerConfiguration;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.*;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.wake.IdentifierFactory;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Main entry point for launching a JobServer for Dolphin applications.
 * See {@link JobServerClient#run(String[])}.
 * This is called by {#start_jobserver.sh}
 */
@ClientSide
public final class JobServerClient {
  private static final Logger LOG = Logger.getLogger(JobServerClient.class.getName());

  private JobServerClient() {

  }

  /**
   * Run a job server on the Dolphin on ET framework with an additional configuration for the driver.
   * @param args command line arguments
   * @param customDriverConf additional Tang configuration to be injected at the driver
   */
  public static LauncherStatus run(final String[] args, final Configuration customDriverConf) {
    LauncherStatus status;

    try {
      // parse command line arguments, separate them into client & driver configuration
      final Pair<Configuration, Configuration> configurations = parseCommandLine(args);

      final Configuration clientParamConf = configurations.getLeft(); // only client uses it
      final Configuration driverParamConf = configurations.getRight(); // only driver uses it
      final Injector clientParameterInjector = Tang.Factory.getTang().newInjector(clientParamConf);
      // runtime configuration
      final boolean onLocal = clientParameterInjector.getNamedInstance(OnLocal.class);
      final Configuration runTimeConf = onLocal ?
          getLocalRuntimeConfiguration(
              clientParameterInjector.getNamedInstance(LocalRuntimeMaxNumEvaluators.class),
              clientParameterInjector.getNamedInstance(JVMHeapSlack.class)) :
          getYarnRuntimeConfiguration(clientParameterInjector.getNamedInstance(JVMHeapSlack.class));

      // driver configuration
      final Configuration driverConf = getDriverConfiguration(driverParamConf, onLocal);
      final int timeout = clientParameterInjector.getNamedInstance(Timeout.class);

      status = JobServerLauncher.getLauncher(runTimeConf)
          .run(Configurations.merge(driverConf, customDriverConf,
              driverParamConf), timeout);

    } catch (final Exception e) {
      status = LauncherStatus.failed(e);
      // This log is for giving more detailed info about failure, which status object does not show
      LOG.log(Level.WARNING, "Exception occurred", e);
    }

    LOG.log(Level.INFO, "REEF job completed: {0}", status);
    return status;
  }

  /**
   * Run a job server on the Dolphin on ET framework.
   * @param args command line arguments
   */
  public static LauncherStatus run(final String[] args) {
    return run(args, ConfigurationUtils.emptyConf());
  }

  @SuppressWarnings("unchecked")
  private static Pair<Configuration, Configuration> parseCommandLine(final String[] args)
      throws ParseException, InjectionException, IOException, ClassNotFoundException {

    final List<Class<? extends Name<?>>> clientParamList = Arrays.asList(
        OnLocal.class, LocalRuntimeMaxNumEvaluators.class, JVMHeapSlack.class, Timeout.class);

    // parameters for driver (job server)
    final List<Class<? extends Name<?>>> driverParamList = Arrays.asList(DriverMemory.class);

    final CommandLine cl = new CommandLine();
    clientParamList.forEach(cl::registerShortNameOfClass);
    driverParamList.forEach(cl::registerShortNameOfClass);

    final Configuration commandLineConf = cl.processCommandLine(args).getBuilder().build();
    final Configuration clientConf = ConfigurationUtils.extractParameterConf(clientParamList, commandLineConf);
    final Configuration driverConf = ConfigurationUtils.extractParameterConf(driverParamList, commandLineConf);
    return Pair.of(clientConf, driverConf);
  }

  private static Configuration getYarnRuntimeConfiguration(final double heapSlack) {
    return YarnClientConfiguration.CONF
        .set(YarnClientConfiguration.JVM_HEAP_SLACK, Double.toString(heapSlack))
        .build();
  }

  private static Configuration getLocalRuntimeConfiguration(final int maxNumEvalLocal, final double heapSlack) {
    return LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, Integer.toString(maxNumEvalLocal))
        .set(LocalRuntimeConfiguration.JVM_HEAP_SLACK, Double.toString(heapSlack))
        .build();
  }

  /**
   * @return a configuration for job server driver
   */
  private static Configuration getDriverConfiguration(final Configuration driverParamConf,
                                                      final boolean onLocal) throws InjectionException {

    final Injector driverParamInjector = Tang.Factory.getTang().newInjector(driverParamConf);

    final Configuration driverConf = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(JobServerDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "JobServer")
        .set(DriverConfiguration.DRIVER_MEMORY, driverParamInjector.getNamedInstance(DriverMemory.class))
        .set(DriverConfiguration.ON_DRIVER_STARTED, JobServerDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_FAILED, JobServerDriver.FailedEvaluatorHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_FAILED, JobServerDriver.FailedContextHandler.class)
        .set(DriverConfiguration.ON_TASK_FAILED, JobServerDriver.FailedTaskHandler.class)
        .set(DriverConfiguration.ON_CLIENT_MESSAGE, JobServerDriver.ClientMessageHandler.class)
        .build();

    final Configuration jobServerConf = Configurations.merge(
        Tang.Factory.getTang().newConfigurationBuilder()
            .bindSetEntry(DriverIdleSources.class, JobServerStatusManager.class)
            .bindNamedParameter(OnLocal.class, String.valueOf(onLocal))
            .build()
    );

    final Configuration etMasterConfiguration = ETDriverConfiguration.CONF.build();

    final Configuration metricServiceConf = MetricServiceDriverConf.CONF
        .set(MetricServiceDriverConf.METRIC_RECEIVER_IMPL, LoggingMetricReceiver.class)
        .build();

    final Configuration driverNetworkConf = NetworkConfProvider.getDriverConfiguration(DriverSideMsgHandler.class);

    return Configurations.merge(driverConf, jobServerConf, etMasterConfiguration, metricServiceConf,
        driverNetworkConf, getNCSConfiguration());
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
    JobServerClient.run(args);
    System.exit(0);
  }
}
