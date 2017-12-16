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
package edu.snu.spl.cruise.ps.jobserver.client;

import edu.snu.spl.cruise.common.param.Parameters.*;
import edu.snu.spl.cruise.ps.CruisePSParameters.*;
import edu.snu.spl.cruise.ps.core.client.CruisePSConfiguration;
import edu.snu.spl.cruise.ps.core.client.CruisePSLauncher;
import edu.snu.spl.cruise.ps.core.master.CruisePSMaster;
import edu.snu.spl.cruise.ps.core.worker.*;
import edu.snu.spl.cruise.ps.jobserver.Parameters.*;
import edu.snu.spl.cruise.ps.metric.parameters.ServerMetricFlushPeriodMs;
import edu.snu.spl.cruise.ps.optimizer.api.OptimizationOrchestrator;
import edu.snu.spl.cruise.ps.optimizer.impl.DummyOrchestrator;
import edu.snu.spl.cruise.services.et.configuration.parameters.KeyCodec;
import edu.snu.spl.cruise.services.et.configuration.parameters.UpdateValueCodec;
import edu.snu.spl.cruise.services.et.configuration.parameters.ValueCodec;
import edu.snu.spl.cruise.services.et.evaluator.api.DataParser;
import edu.snu.spl.cruise.services.et.evaluator.api.UpdateFunction;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.spl.cruise.utils.ConfigurationUtils.extractParameterConf;

/**
 * A class that submits a specific ML job dynamically to job server via {@link JobServerClient}.
 * It communicates with {@link JobServerClient}
 * through the connection between {@link CommandSender} and {@link CommandListener}.
 *
 * Users can run different apps with different parameters by changing
 * args and cruise configuration for {@link #submitJob(String, String[], CruisePSConfiguration)}.
 */
@ClientSide
public final class JobLauncher {

  private static final Logger LOG = Logger.getLogger(JobLauncher.class.getName());

  // utility class should not be instantiated
  private JobLauncher() {

  }

  /**
   * Submits a job to JobServer.
   * @param appId an app id
   * @param args arguments for app
   * @param cruiseConf cruise configuration
   */
  public static void submitJob(final String appId,
                               final String[] args,
                               final CruisePSConfiguration cruiseConf) {
    try {

      final List<Configuration> configurations = parseCommandLine(args, cruiseConf.getParameterClassList());
      final Configuration masterParamConf = configurations.get(0);
      final Configuration serverParamConf = configurations.get(1);
      final Configuration workerParamConf = configurations.get(2);
      final Configuration userParamConf = configurations.get(3);

      // server conf. servers will be spawned with this configuration
      final Configuration serverConf = Configurations.merge(
          serverParamConf, userParamConf,
          Tang.Factory.getTang().newConfigurationBuilder()
              .bindImplementation(UpdateFunction.class, cruiseConf.getModelUpdateFunctionClass())
              .bindNamedParameter(KeyCodec.class, cruiseConf.getModelKeyCodecClass())
              .bindNamedParameter(ValueCodec.class, cruiseConf.getModelValueCodecClass())
              .bindNamedParameter(UpdateValueCodec.class, cruiseConf.getModelUpdateValueCodecClass())
              .build());

      // worker conf. workers will be spawned with this configuration
      final Configuration workerConf = Configurations.merge(
          workerParamConf, userParamConf,
          Tang.Factory.getTang().newConfigurationBuilder()
              .bindImplementation(Trainer.class, cruiseConf.getTrainerClass())
              .bindImplementation(DataParser.class, cruiseConf.getInputParserClass())
              .bindImplementation(TrainingDataProvider.class, ETTrainingDataProvider.class)
              .bindImplementation(ModelAccessor.class, ETModelAccessor.class)
              .bindNamedParameter(KeyCodec.class, cruiseConf.getInputKeyCodecClass())
              .bindNamedParameter(ValueCodec.class, cruiseConf.getInputValueCodecClass())
              .build());

      // job configuration. driver will use this configuration to spawn a job
      final Configuration jobConf = getJobConfiguration(appId, masterParamConf, serverConf, workerConf, userParamConf);

      final CommandSender commandSender =
          Tang.Factory.getTang().newInjector().getInstance(CommandSender.class);

      LOG.log(Level.INFO, "Submit {0}", appId);
      commandSender.sendJobSubmitCommand(Configurations.toString(jobConf));

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static List<Configuration> parseCommandLine(
      final String[] args, final List<Class<? extends Name<?>>> customAppParamList)
      throws IOException, InjectionException, ClassNotFoundException {

    // parameters for master
    final List<Class<? extends Name<?>>> masterParamList = Arrays.asList(
        MaxNumEpochs.class, NumTotalMiniBatches.class, NumWorkers.class, ServerMetricFlushPeriodMs.class
    );

    // commonly used parameters for ML apps
    final List<Class<? extends Name<?>>> commonAppParamList = Arrays.asList(
        NumFeatures.class, Lambda.class, DecayRate.class, DecayPeriod.class, StepSize.class,
        ModelGaussian.class, NumFeaturesPerPartition.class
    );

    // user param list is composed by common app parameters and custom app parameters
    final List<Class<? extends Name<?>>> userParamList = new ArrayList<>(commonAppParamList);
    userParamList.addAll(customAppParamList);

    // parameters for servers
    final List<Class<? extends Name<?>>> serverParamList = Arrays.asList(
        NumServers.class, ServerMemSize.class, NumServerCores.class,
        NumServerHandlerThreads.class, NumServerSenderThreads.class,
        ServerHandlerQueueSize.class, ServerSenderQueueSize.class,
        NumServerBlocks.class, ServerMetricFlushPeriodMs.class
    );

    // parameters for workers
    final List<Class<? extends Name<?>>> workerParamList = Arrays.asList(
        NumWorkers.class, WorkerMemSize.class, NumWorkerCores.class,
        NumWorkerHandlerThreads.class, NumWorkerSenderThreads.class,
        WorkerHandlerQueueSize.class, WorkerSenderQueueSize.class,
        NumWorkerBlocks.class, HyperThreadEnabled.class, MaxNumEpochs.class,
        NumTotalMiniBatches.class, TestDataPath.class, InputDir.class
    );

    final CommandLine cl = new CommandLine();
    userParamList.forEach(cl::registerShortNameOfClass);
    serverParamList.forEach(cl::registerShortNameOfClass);
    workerParamList.forEach(cl::registerShortNameOfClass);
    // master-side params are already included in server/worker params

    final Configuration commandLineConf = cl.processCommandLine(args).getBuilder().build();
    final Configuration masterConf = extractParameterConf(masterParamList, commandLineConf);
    final Configuration serverConf = extractParameterConf(serverParamList, commandLineConf);
    final Configuration workerConf = extractParameterConf(workerParamList, commandLineConf);
    final Configuration userConf = extractParameterConf(userParamList, commandLineConf);

    return Arrays.asList(masterConf, serverConf, workerConf, userConf);
  }

  /**
   * @return a configuration for spawning a {@link CruisePSMaster}.
   */
  private static Configuration getJobConfiguration(final String appId,
                                                   final Configuration masterConf,
                                                   final Configuration serverConf,
                                                   final Configuration workerConf,
                                                   final Configuration userParamConf) {
    return Configurations.merge(masterConf, Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(AppIdentifier.class, appId)
        .bindImplementation(OptimizationOrchestrator.class, DummyOrchestrator.class)
        .bindNamedParameter(CruisePSLauncher.SerializedServerConf.class, Configurations.toString(serverConf))
        .bindNamedParameter(CruisePSLauncher.SerializedWorkerConf.class, Configurations.toString(workerConf))
        .bindNamedParameter(CruisePSLauncher.SerializedParamConf.class, Configurations.toString(userParamConf))
        .build());
  }
}
