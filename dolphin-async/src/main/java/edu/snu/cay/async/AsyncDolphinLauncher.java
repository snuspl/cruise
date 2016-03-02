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
package edu.snu.cay.async;

import edu.snu.cay.common.aggregation.AggregationConfiguration;
import edu.snu.cay.common.param.Parameters.*;
import edu.snu.cay.services.dataloader.DataLoadingRequestBuilder;
import edu.snu.cay.services.ps.ParameterServerConfigurationBuilder;
import edu.snu.cay.services.ps.driver.impl.ConcurrentParameterServerManager;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.io.network.naming.LocalNameResolverConfiguration;
import org.apache.reef.io.network.naming.NameServerConfiguration;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.wake.IdentifierFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Main entry point for launching a {@code dolphin-async} application.
 * See {@link AsyncDolphinLauncher#launch(String, String[], AsyncDolphinConfiguration)}.
 */
@ClientSide
public final class AsyncDolphinLauncher {
  private static final Logger LOG = Logger.getLogger(AsyncDolphinLauncher.class.getName());

  @NamedParameter(doc = "configuration for parameters, serialized as a string")
  final class SerializedParameterConfiguration implements Name<String> {
  }

  @NamedParameter(doc = "configuration for worker class, serialized as a string")
  final class SerializedWorkerConfiguration implements Name<String> {
  }

  /**
   * Should not be instantiated.
   */
  private AsyncDolphinLauncher() {
  }

  /**
   * Launch an application on the {@code dolphin-async} framework with an additional configuration for the driver.
   * @param jobName string identifier of this application
   * @param args command line arguments
   * @param asyncDolphinConfiguration job configuration of this application
   * @param customDriverConfiguration additional Tang configuration to be injected at the driver
   */
  public static LauncherStatus launch(final String jobName,
                                      final String[] args,
                                      final AsyncDolphinConfiguration asyncDolphinConfiguration,
                                      final Configuration customDriverConfiguration) {
    try {
      // parse command line arguments
      final Configuration commandLineConf = parseCommandLine(args, asyncDolphinConfiguration.getParameterClassList());
      final Injector commandLineInjector = Tang.Factory.getTang().newInjector(commandLineConf);

      // local or yarn runtime
      final boolean onLocal = commandLineInjector.getNamedInstance(OnLocal.class);
      final Configuration runTimeConf = onLocal ?
          getLocalRuntimeConfiguration(commandLineInjector.getNamedInstance(LocalRuntimeMaxNumEvaluators.class)) :
          getYarnRuntimeConfiguration();

      // configuration for the parameter server
      final Configuration parameterServerConf = ParameterServerConfigurationBuilder.newBuilder()
          .setManagerClass(ConcurrentParameterServerManager.class)
          .setUpdaterClass(asyncDolphinConfiguration.getUpdaterClass())
          .setKeyCodecClass(asyncDolphinConfiguration.getKeyCodecClass())
          .setPreValueCodecClass(asyncDolphinConfiguration.getPreValueCodecClass())
          .setValueCodecClass(asyncDolphinConfiguration.getValueCodecClass())
          .build();

      // worker-specific configurations
      final ConfigurationSerializer confSerializer = new AvroConfigurationSerializer();
      // pass the worker class implementation as well as all command line arguments
      final Configuration workerConf = Tang.Factory.getTang().newConfigurationBuilder()
          .bindImplementation(Worker.class, asyncDolphinConfiguration.getWorkerClass())
          .build();
      final Configuration serializedWorkerConf = Tang.Factory.getTang().newConfigurationBuilder()
          .bindNamedParameter(SerializedWorkerConfiguration.class, confSerializer.toString(workerConf))
          .bindNamedParameter(SerializedParameterConfiguration.class, confSerializer.toString(commandLineConf))
          .build();

      // driver-side configurations
      final Configuration driverConf = getDriverConfiguration(jobName, commandLineInjector);
      final int timeout = commandLineInjector.getNamedInstance(Timeout.class);

      final LauncherStatus status = DriverLauncher.getLauncher(runTimeConf).run(
          Configurations.merge(parameterServerConf, serializedWorkerConf, driverConf, customDriverConfiguration),
          timeout);
      LOG.log(Level.INFO, "REEF job completed: {0}", status);
      return status;

    } catch (final Exception e) {
      final LauncherStatus status = LauncherStatus.failed(e);
      LOG.log(Level.INFO, "REEF job completed: {0}", status);
      return status;
    }
  }

  /**
   * Launch an application on the {@code dolphin-async} framework.
   * @param jobName string identifier of this application
   * @param args command line arguments
   * @param asyncDolphinConfiguration job configuration of this application
   */
  public static LauncherStatus launch(final String jobName,
                                      final String[] args,
                                      final AsyncDolphinConfiguration asyncDolphinConfiguration) {
    return launch(jobName, args, asyncDolphinConfiguration, Tang.Factory.getTang().newConfigurationBuilder().build());
  }

  private static Configuration parseCommandLine(
      final String[] args,
      final List<Class<? extends Name<?>>> parameterClassList) throws IOException {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    final CommandLine cl = new CommandLine(cb);
    // add all basic parameters
    cl.registerShortNameOfClass(EvaluatorSize.class);
    cl.registerShortNameOfClass(InputDir.class);
    cl.registerShortNameOfClass(OnLocal.class);
    cl.registerShortNameOfClass(Splits.class);
    cl.registerShortNameOfClass(Timeout.class);
    cl.registerShortNameOfClass(LocalRuntimeMaxNumEvaluators.class);
    cl.registerShortNameOfClass(Iterations.class);

    // also add any additional parameters specified by user
    for (final Class<? extends Name<?>> workerParameterClass : parameterClassList) {
      cl.registerShortNameOfClass(workerParameterClass);
    }

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

  private static Configuration getDriverConfiguration(
      final String jobName, final Injector injector) throws InjectionException {
    final ConfigurationModule driverConf = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(AsyncDolphinDriver.class))
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(TextInputFormat.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, jobName)
        .set(DriverConfiguration.ON_DRIVER_STARTED, AsyncDolphinDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, AsyncDolphinDriver.AllocatedEvaluatorHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_FAILED, AsyncDolphinDriver.FailedEvaluatorHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, AsyncDolphinDriver.ActiveContextHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_FAILED, AsyncDolphinDriver.FailedContextHandler.class)
        .set(DriverConfiguration.ON_TASK_COMPLETED, AsyncDolphinDriver.CompletedTaskHandler.class)
        .set(DriverConfiguration.ON_TASK_FAILED, AsyncDolphinDriver.FailedTaskHandler.class);

    final Configuration driverConfWithDataLoad = new DataLoadingRequestBuilder()
        .setInputFormatClass(TextInputFormat.class)
        .setInputPath(processInputDir(injector.getNamedInstance(InputDir.class), injector))
        .setNumberOfDesiredSplits(injector.getNamedInstance(Splits.class))
        .setDriverConfigurationModule(driverConf)
        .build();

    final AggregationConfiguration aggregationServiceConf = AggregationConfiguration.newBuilder()
        .addAggregationClient(SynchronizationManager.AGGREGATION_CLIENT_NAME,
            SynchronizationManager.MessageHandler.class,
            WorkerSynchronizer.MessageHandler.class)
        .build();

    return Configurations.merge(driverConfWithDataLoad,
        aggregationServiceConf.getDriverConfiguration(),
        NameServerConfiguration.CONF.build(),
        LocalNameResolverConfiguration.CONF.build(),
        Tang.Factory.getTang().newConfigurationBuilder()
            .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
            .build());
  }

  private static String processInputDir(final String inputDir, final Injector injector) throws InjectionException {
    if (!injector.getNamedInstance(OnLocal.class)) {
      return inputDir;
    }
    final File inputFile = new File(inputDir);
    return "file:///" + inputFile.getAbsolutePath();
  }
}
