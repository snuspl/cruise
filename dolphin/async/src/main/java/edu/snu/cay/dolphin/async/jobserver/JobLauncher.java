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
import edu.snu.cay.dolphin.async.*;
import edu.snu.cay.dolphin.async.DolphinParameters.*;
import edu.snu.cay.dolphin.async.jobserver.Parameters.*;
import edu.snu.cay.dolphin.async.optimizer.api.OptimizationOrchestrator;
import edu.snu.cay.dolphin.async.optimizer.impl.DummyOrchestrator;
import edu.snu.cay.services.et.configuration.parameters.KeyCodec;
import edu.snu.cay.services.et.configuration.parameters.UpdateValueCodec;
import edu.snu.cay.services.et.configuration.parameters.ValueCodec;
import edu.snu.cay.services.et.evaluator.api.DataParser;
import edu.snu.cay.services.et.evaluator.api.UpdateFunction;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.tang.types.NamedParameterNode;

import java.io.*;
import java.util.Arrays;
import java.util.List;

/**
 * JobLauncher, which submits specific ML job dynamically to running job server via HTTP request.
 * All parameters related to job are determined by command line.
 * Note that it supports only NMF job in this stage.
 */
@ClientSide
public final class JobLauncher {

  private JobLauncher() {

  }

  /**
   * Submits a job to JobServer.
   * @param appId an app id
   * @param args arguments for app
   * @param dolphinConf dolphin configuration
   */
  public static void submitJob(final String appId,
                               final String[] args,
                               final ETDolphinConfiguration dolphinConf) {
    try {
      final List<Configuration> configurations = parseCommandLine(args, dolphinConf.getParameterClassList());
      final Configuration masterParamConf = configurations.get(0);
      final Configuration serverParamConf = configurations.get(1);
      final Configuration workerParamConf = configurations.get(2);
      final Configuration userParamConf = configurations.get(3);
      final Configuration httpConf = configurations.get(4);

      // server conf. servers will be spawned with this configuration
      final Configuration serverConf = Configurations.merge(
          serverParamConf, userParamConf,
          Tang.Factory.getTang().newConfigurationBuilder()
              .bindImplementation(UpdateFunction.class, dolphinConf.getModelUpdateFunctionClass())
              .bindNamedParameter(KeyCodec.class, dolphinConf.getModelKeyCodecClass())
              .bindNamedParameter(ValueCodec.class, dolphinConf.getModelValueCodecClass())
              .bindNamedParameter(UpdateValueCodec.class, dolphinConf.getModelUpdateValueCodecClass())
              .build());

      // worker conf. workers will be spawned with this configuration
      final Configuration workerConf = Configurations.merge(
          workerParamConf, userParamConf,
          Tang.Factory.getTang().newConfigurationBuilder()
              .bindImplementation(Trainer.class, dolphinConf.getTrainerClass())
              .bindImplementation(DataParser.class, dolphinConf.getInputParserClass())
              .bindImplementation(TrainingDataProvider.class, ETTrainingDataProvider.class)
              .bindImplementation(ModelAccessor.class, ETModelAccessor.class)
              .bindNamedParameter(KeyCodec.class, dolphinConf.getInputKeyCodecClass())
              .bindNamedParameter(ValueCodec.class, dolphinConf.getInputValueCodecClass())
              .build());

      // job configuration. driver will use this configuration to spawn a job
      final Configuration jobConf = getJobConfiguration(appId, masterParamConf, serverConf, workerConf, userParamConf);

      // send http request
      final ConfigurationSerializer configurationSerializer = new AvroConfigurationSerializer();
      final Injector httpConfInjector = Tang.Factory.getTang().newInjector(httpConf);
      final String targetAddress = httpConfInjector.getNamedInstance(HttpAddress.class);
      final String targetPort = httpConfInjector.getNamedInstance(HttpPort.class);
      HttpSender.sendSubmitCommand(targetAddress, targetPort, configurationSerializer.toString(jobConf));

    } catch (IOException | InjectionException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private static List<Configuration> parseCommandLine(
      final String[] args, final List<Class<? extends Name<?>>> userParamList)
      throws IOException, InjectionException, ClassNotFoundException {

    // parameters for master
    final List<Class<? extends Name<?>>> masterParamList = Arrays.asList(
        MaxNumEpochs.class, MiniBatchSize.class, NumWorkers.class
    );

    // parameters for servers
    final List<Class<? extends Name<?>>> serverParamList = Arrays.asList(
        NumServers.class, ServerMemSize.class, NumServerCores.class,
        NumServerHandlerThreads.class, NumServerSenderThreads.class,
        ServerHandlerQueueSize.class, ServerSenderQueueSize.class,
        NumServerBlocks.class
    );

    // parameters for workers
    final List<Class<? extends Name<?>>> workerParamList = Arrays.asList(
        NumWorkers.class, WorkerMemSize.class, NumWorkerCores.class,
        NumWorkerHandlerThreads.class, NumWorkerSenderThreads.class,
        WorkerHandlerQueueSize.class, WorkerSenderQueueSize.class,
        NumWorkerBlocks.class, NumTrainerThreads.class, MaxNumEpochs.class, MiniBatchSize.class, TestDataPath.class
    );

    final CommandLine cl = new CommandLine();
    cl.registerShortNameOfClass(HttpAddress.class);
    cl.registerShortNameOfClass(HttpPort.class);
    serverParamList.forEach(cl::registerShortNameOfClass);
    workerParamList.forEach(cl::registerShortNameOfClass);
    cl.registerShortNameOfClass(InputDir.class); // handle inputPath separately to process it through processInputDir()
    userParamList.forEach(cl::registerShortNameOfClass);

    final Configuration commandLineConf = cl.processCommandLine(args).getBuilder().build();

    // master side parameters are already registered. So it can be extracted
    // from commandLineConf unless it wasn't registered.
    final Configuration masterConf = extractParameterConf(masterParamList, commandLineConf);
    final Configuration serverConf = extractParameterConf(serverParamList, commandLineConf);
    final Configuration workerConf = extractParameterConf(workerParamList, commandLineConf);
    final Configuration userConf = extractParameterConf(userParamList, commandLineConf);

    // handle special parameters that need to be processed from commandline parameters
    final Injector commandlineParamInjector = Tang.Factory.getTang().newInjector(commandLineConf);
    final Configuration inputPathConf;
    final String inputPath = commandlineParamInjector.getNamedInstance(InputDir.class);
    inputPathConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(InputDir.class, inputPath)
        .build();

    // http configuration, target of http request is specified by this configuration.
    final Configuration httpConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(HttpAddress.class, commandlineParamInjector.getNamedInstance(HttpAddress.class))
        .bindNamedParameter(HttpPort.class, commandlineParamInjector.getNamedInstance(HttpPort.class))
        .build();

    return Arrays.asList(masterConf, serverConf,
        Configurations.merge(workerConf, inputPathConf), userConf, httpConf);
  }

  /**
   * Extracts configuration which is only related to {@code parameterClassList} from {@code totalConf}.
   */
  private static Configuration extractParameterConf(final List<Class<? extends Name<?>>> parameterClassList,
                                                    final Configuration totalConf) {
    final ClassHierarchy totalConfClassHierarchy = totalConf.getClassHierarchy();
    final JavaConfigurationBuilder parameterConfBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    for (final Class<? extends Name<?>> parameterClass : parameterClassList) {
      final NamedParameterNode parameterNode
          = (NamedParameterNode) totalConfClassHierarchy.getNode(parameterClass.getName());
      final String parameterValue = totalConf.getNamedParameter(parameterNode);
      // if this parameter is not included in the total configuration, parameterValue will be null
      if (parameterValue != null) {
        parameterConfBuilder.bindNamedParameter(parameterClass, parameterValue);
      }
    }
    return parameterConfBuilder.build();
  }

  /**
   * @return a configuration for spawning a {@link DolphinMaster}.
   */
  private static Configuration getJobConfiguration(final String appId,
                                                   final Configuration masterConf,
                                                   final Configuration serverConf,
                                                   final Configuration workerConf,
                                                   final Configuration userParamConf) {
    final ConfigurationSerializer confSerializer = new AvroConfigurationSerializer();
    return Configurations.merge(masterConf, Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(AppIdentifier.class, appId)
        .bindImplementation(OptimizationOrchestrator.class, DummyOrchestrator.class)
        .bindNamedParameter(ETDolphinLauncher.SerializedServerConf.class, confSerializer.toString(serverConf))
        .bindNamedParameter(ETDolphinLauncher.SerializedWorkerConf.class, confSerializer.toString(workerConf))
        .bindNamedParameter(ETDolphinLauncher.SerializedParamConf.class, confSerializer.toString(userParamConf))
        .build());
  }
}
