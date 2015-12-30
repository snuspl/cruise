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

import edu.snu.cay.async.AsyncParameters.*;
import edu.snu.cay.services.dataloader.DataLoadingRequestBuilder;
import edu.snu.cay.services.ps.ParameterServerConfigurationBuilder;
import edu.snu.cay.services.ps.driver.impl.SingleNodeParameterServerManager;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
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

import javax.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Main entry point for launching a {@code dolphin-async} application.
 * See {@link AsyncLauncher#launch(String, String[], AsyncConfiguration)}.
 */
@ClientSide
public final class AsyncLauncher {
  private static final Logger LOG = Logger.getLogger(AsyncLauncher.class.getName());

  @NamedParameter(doc = "configuration for workers serialized as a string")
  final class SerializedWorkerConfiguration implements Name<String> {
  }

  /**
   * Should not be instantiated.
   */
  @Inject
  private AsyncLauncher() {
  }

  /**
   * Launch an application on the {@code dolphin-async} framework.
   * @param jobName string identifier of this application
   * @param args command line arguments
   * @param asyncConfiguration job configuration of this application
   */
  public static void launch(final String jobName, final String[] args, final AsyncConfiguration asyncConfiguration) {
    try {
      // parse command line arguments
      final Configuration commandLineConf = parseCommandLine(args, asyncConfiguration.getWorkerParameterClassList());
      final Injector commandLineInjector = Tang.Factory.getTang().newInjector(commandLineConf);

      // local or yarn runtime
      final boolean onLocal = commandLineInjector.getNamedInstance(OnLocal.class);
      final Configuration runTimeConf = onLocal ?
          getLocalRuntimeConfiguration(commandLineInjector.getNamedInstance(LocalRuntimeMaxNumEvaluators.class)) :
          getYarnRuntimeConfiguration();

      // configuration for the parameter server
      final Configuration parameterServerConf = ParameterServerConfigurationBuilder.newBuilder()
          .setManagerClass(SingleNodeParameterServerManager.class)
          .setUpdaterClass(asyncConfiguration.getUpdaterClass())
          .setKeyCodecClass(asyncConfiguration.getKeyCodecClass())
          .setPreValueCodecClass(asyncConfiguration.getPreValueCodecClass())
          .setValueCodecClass(asyncConfiguration.getValueCodecClass())
          .build();

      // worker-specific configurations
      final ConfigurationSerializer confSerializer = new AvroConfigurationSerializer();
      // pass the worker class implementation as well as all command line arguments
      final Configuration workerConf = Tang.Factory.getTang().newConfigurationBuilder(commandLineConf)
          .bindImplementation(Worker.class, asyncConfiguration.getWorkerClass())
          .build();
      final Configuration serializedWorkerConf = Tang.Factory.getTang().newConfigurationBuilder()
          .bindNamedParameter(SerializedWorkerConfiguration.class, confSerializer.toString(workerConf))
          .build();

      // driver-side configurations
      final Configuration driverConf = getDriverConfiguration(jobName, commandLineInjector);
      final int timeout = commandLineInjector.getNamedInstance(Timeout.class);

      final LauncherStatus status = DriverLauncher.getLauncher(runTimeConf).run(
          Configurations.merge(parameterServerConf, serializedWorkerConf, driverConf),
          timeout);
      LOG.log(Level.INFO, "REEF job completed: {0}", status);

    } catch (final Exception e) {
      LOG.log(Level.INFO, "REEF job completed: {0}", LauncherStatus.failed(e));
    }
  }

  private static Configuration parseCommandLine(
      final String[] args,
      final List<Class<? extends Name<?>>> workerParameterClassArray) throws IOException {
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
    for (final Class<? extends Name<?>> workerParameterClass : workerParameterClassArray) {
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
    final int evalSize = injector.getNamedInstance(EvaluatorSize.class);

    final ConfigurationModule driverConf = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(AsyncDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, jobName)
        .set(DriverConfiguration.ON_DRIVER_STARTED, AsyncDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, AsyncDriver.AllocatedEvaluatorHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, AsyncDriver.ActiveContextHandler.class)
        .set(DriverConfiguration.ON_TASK_COMPLETED, AsyncDriver.CompletedTaskHandler.class)
        .set(DriverConfiguration.ON_TASK_FAILED, AsyncDriver.FailedTaskHandler.class);

    // one evaluator for the parameter server
    final EvaluatorRequest compRequest = EvaluatorRequest.newBuilder()
        .setNumber(1)
        .setMemory(evalSize)
        .build();

    // We do not explicitly set the number of data loading evaluators here, because
    // the number is being reset to the number of data partitions at the Driver in DataLoader anyway.
    final EvaluatorRequest dataRequest = EvaluatorRequest.newBuilder()
        .setMemory(evalSize)
        .build();

    return new DataLoadingRequestBuilder()
        .setInputFormatClass(TextInputFormat.class)
        .setInputPath(injector.getNamedInstance(InputDir.class))
        .setNumberOfDesiredSplits(injector.getNamedInstance(Splits.class))
        .addComputeRequest(compRequest)
        .addDataRequest(dataRequest)
        .setDriverConfigurationModule(driverConf)
        .build();
  }
}
