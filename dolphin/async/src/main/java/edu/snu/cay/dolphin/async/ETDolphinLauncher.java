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
package edu.snu.cay.dolphin.async;
import edu.snu.cay.common.client.DriverLauncher;
import edu.snu.cay.dolphin.async.DolphinParameters.*;
import edu.snu.cay.common.param.Parameters.*;
import edu.snu.cay.dolphin.async.metric.ETDolphinMetricReceiver;
import edu.snu.cay.dolphin.async.metric.parameters.ServerMetricFlushPeriodMs;
import edu.snu.cay.dolphin.async.network.NetworkConfProvider;
import edu.snu.cay.dolphin.async.optimizer.api.Optimizer;
import edu.snu.cay.dolphin.async.optimizer.conf.OptimizerClass;
import edu.snu.cay.dolphin.async.optimizer.parameters.*;
import edu.snu.cay.dolphin.async.plan.impl.ETPlanExecutorClass;
import edu.snu.cay.services.et.configuration.ETDriverConfiguration;
import edu.snu.cay.services.et.configuration.parameters.KeyCodec;
import edu.snu.cay.services.et.configuration.parameters.UpdateValueCodec;
import edu.snu.cay.services.et.configuration.parameters.ValueCodec;
import edu.snu.cay.services.et.evaluator.api.UpdateFunction;
import edu.snu.cay.services.et.evaluator.api.DataParser;
import edu.snu.cay.services.et.metric.configuration.MetricServiceDriverConf;
import edu.snu.cay.services.et.plan.api.PlanExecutor;
import org.apache.commons.cli.ParseException;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.client.DriverConfiguration;
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
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.tang.types.NamedParameterNode;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.wake.IdentifierFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Main entry point for launching a Dolphin on ET application.
 * See {@link ETDolphinLauncher#launch(String, String[], ETDolphinConfiguration)}.
 */
@ClientSide
public final class ETDolphinLauncher {
  private static final Logger LOG = Logger.getLogger(ETDolphinLauncher.class.getName());

  @NamedParameter(doc = "configuration for parameters, serialized as a string")
  public final class SerializedParamConf implements Name<String> {
  }

  @NamedParameter(doc = "configuration for worker class, serialized as a string")
  public final class SerializedWorkerConf implements Name<String> {
  }

  @NamedParameter(doc = "configuration for server class, serialized as a string")
  public final class SerializedServerConf implements Name<String> {
  }

  /**
   * Should not be instantiated.
   */
  private ETDolphinLauncher() {
  }

  /**
   * Launch an application on the Dolphin on ET framework with an additional configuration for the driver.
   * @param jobName string identifier of this application
   * @param args command line arguments
   * @param dolphinConf job configuration of this application
   * @param customDriverConf additional Tang configuration to be injected at the driver
   */
  public static LauncherStatus launch(final String jobName,
                                      final String[] args,
                                      final ETDolphinConfiguration dolphinConf,
                                      final Configuration customDriverConf) {
    LauncherStatus status;

    try {
      // parse command line arguments, separate them into basic & user parameters
      final List<Configuration> configurations = parseCommandLine(args, dolphinConf.getParameterClassList());

      final Configuration clientParamConf = configurations.get(0);
      final Configuration driverParamConf = configurations.get(1);
      final Configuration serverParamConf = configurations.get(2);
      final Configuration workerParamConf = configurations.get(3);
      final Configuration userParamConf = configurations.get(4);

      // server conf
      final Configuration serverConf = Configurations.merge(
          serverParamConf, userParamConf,
          Tang.Factory.getTang().newConfigurationBuilder()
              .bindImplementation(UpdateFunction.class, dolphinConf.getModelUpdateFunctionClass())
              .bindNamedParameter(KeyCodec.class, dolphinConf.getModelKeyCodecClass())
              .bindNamedParameter(ValueCodec.class, dolphinConf.getModelValueCodecClass())
              .bindNamedParameter(UpdateValueCodec.class, dolphinConf.getModelUpdateValueCodecClass())
              .build());

      // worker conf
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

      final Injector clientParameterInjector = Tang.Factory.getTang().newInjector(clientParamConf);
      // runtime configuration
      final boolean onLocal = clientParameterInjector.getNamedInstance(OnLocal.class);
      final Configuration runTimeConf = onLocal ?
          getLocalRuntimeConfiguration(
              clientParameterInjector.getNamedInstance(LocalRuntimeMaxNumEvaluators.class),
              clientParameterInjector.getNamedInstance(JVMHeapSlack.class)) :
          getYarnRuntimeConfiguration(clientParameterInjector.getNamedInstance(JVMHeapSlack.class));

      // driver configuration
      final Configuration driverConf = getDriverConfiguration(jobName,
          clientParameterInjector.getNamedInstance(DriverMemory.class),
          serverConf, workerConf, userParamConf);

      final int timeout = clientParameterInjector.getNamedInstance(Timeout.class);

      status = DriverLauncher.getLauncher(runTimeConf)
          .run(Configurations.merge(driverConf, customDriverConf,
              driverParamConf, workerParamConf, serverParamConf, userParamConf), timeout);

    } catch (final Exception e) {
      status = LauncherStatus.failed(e);
      // This log is for giving more detailed info about failure, which status object does not show
      LOG.log(Level.WARNING, "Exception occurred", e);
    }

    LOG.log(Level.INFO, "REEF job completed: {0}", status);
    return status;
  }

  /**
   * Launch an application on the Dolphin on ET framework.
   * @param jobName string identifier of this application
   * @param args command line arguments
   * @param etDolphinConfiguration job configuration of this application
   */
  public static LauncherStatus launch(final String jobName,
                                      final String[] args,
                                      final ETDolphinConfiguration etDolphinConfiguration) {
    return launch(jobName, args, etDolphinConfiguration, Tang.Factory.getTang().newConfigurationBuilder().build());
  }

  @SuppressWarnings("unchecked")
  private static List<Configuration> parseCommandLine(
      final String[] args, final List<Class<? extends Name<?>>> userParamList)
      throws ParseException, InjectionException, IOException, ClassNotFoundException {

    final List<Class<? extends Name<?>>> clientParamList = Arrays.asList(
        OnLocal.class, LocalRuntimeMaxNumEvaluators.class, JVMHeapSlack.class, DriverMemory.class, Timeout.class);

    final List<Class<? extends Name<?>>> driverParamList = Arrays.asList(
        // generic params
        NumServers.class, ServerMemSize.class, NumServerCores.class,
        NumWorkers.class, WorkerMemSize.class, NumWorkerCores.class,

        // ET-specific params
        NumServerHandlerThreads.class, NumServerSenderThreads.class,
        ServerHandlerQueueSize.class, ServerSenderQueueSize.class,
        NumWorkerHandlerThreads.class, NumWorkerSenderThreads.class,
        WorkerHandlerQueueSize.class, WorkerSenderQueueSize.class,
        NumServerBlocks.class, NumWorkerBlocks.class,

        // optimization params
        DelayAfterOptimizationMs.class, OptimizationIntervalMs.class, OptimizationBenefitThreshold.class,

        // metric processing params
        MovingAverageWindowSize.class, MetricWeightFactor.class,

        // extra resource params
        NumExtraResources.class, ExtraResourcesPeriodSec.class,

        // metric collection params
        ServerMetricFlushPeriodMs.class);

    // it's empty now
    final List<Class<? extends Name<?>>> serverParamList = Collections.emptyList();

    final List<Class<? extends Name<?>>> workerParamList = Arrays.asList(
        NumTrainerThreads.class, MaxNumEpochs.class, NumTotalMiniBatches.class, TestDataPath.class);

    final CommandLine cl = new CommandLine();
    clientParamList.forEach(cl::registerShortNameOfClass);
    driverParamList.forEach(cl::registerShortNameOfClass);
    cl.registerShortNameOfClass(OptimizerClass.class); // handle it separately to bind a corresponding implementation
    cl.registerShortNameOfClass(ETPlanExecutorClass.class); // handle it separately similar to OptimizerClass
    serverParamList.forEach(cl::registerShortNameOfClass);
    workerParamList.forEach(cl::registerShortNameOfClass);
    cl.registerShortNameOfClass(InputDir.class); // handle inputPath separately to process it through processInputDir()
    userParamList.forEach(cl::registerShortNameOfClass);

    final Configuration commandLineConf = cl.processCommandLine(args).getBuilder().build();
    final Configuration clientConf = extractParameterConf(clientParamList, commandLineConf);
    final Configuration driverConf = extractParameterConf(driverParamList, commandLineConf);
    final Configuration serverConf = extractParameterConf(serverParamList, commandLineConf);
    final Configuration workerConf = extractParameterConf(workerParamList, commandLineConf);
    final Configuration userConf = extractParameterConf(userParamList, commandLineConf);

    // handle special parameters that need to be processed from commandline parameters
    final Injector commandlineParamInjector = Tang.Factory.getTang().newInjector(commandLineConf);

    final Configuration optimizationConf;
    final int numInitialResources = commandlineParamInjector.getNamedInstance(NumWorkers.class)
        + commandlineParamInjector.getNamedInstance(NumServers.class);

    final Class<? extends Optimizer> optimizerClass =
        (Class<? extends Optimizer>) Class.forName(commandlineParamInjector.getNamedInstance(OptimizerClass.class));

    final Class<? extends PlanExecutor> planExecutorClass = (Class<? extends PlanExecutor>)
        Class.forName(commandlineParamInjector.getNamedInstance(ETPlanExecutorClass.class));

    optimizationConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(Optimizer.class, optimizerClass)
        .bindImplementation(PlanExecutor.class, planExecutorClass)
        .bindNamedParameter(NumInitialResources.class, Integer.toString(numInitialResources))
        .build();

    final Configuration inputPathConf;
    final boolean onLocal = commandlineParamInjector.getNamedInstance(OnLocal.class);
    final String inputPath = commandlineParamInjector.getNamedInstance(InputDir.class);
    final String processedInputPath = processInputDir(inputPath, onLocal);
    inputPathConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(InputDir.class, processedInputPath)
        .build();

    return Arrays.asList(clientConf, Configurations.merge(driverConf, optimizationConf), serverConf,
        Configurations.merge(workerConf, inputPathConf), userConf);
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

  private static Configuration getDriverConfiguration(final String jobName,
                                                      final int driverMemSize,
                                                      final Configuration serverConf,
                                                      final Configuration workerConf,
                                                      final Configuration userParamConf) {
    final Configuration driverConf = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(DolphinDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, jobName)
        .set(DriverConfiguration.DRIVER_MEMORY, driverMemSize)
        .set(DriverConfiguration.ON_DRIVER_STARTED, DolphinDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_FAILED, DolphinDriver.FailedEvaluatorHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_FAILED, DolphinDriver.FailedContextHandler.class)
        .set(DriverConfiguration.ON_TASK_FAILED, DolphinDriver.FailedTaskHandler.class)
        .set(DriverConfiguration.PROGRESS_PROVIDER, ProgressTracker.class)
        .build();

    final Configuration etMasterConfiguration = ETDriverConfiguration.CONF.build();

    final Configuration metricServiceConf = MetricServiceDriverConf.CONF
        .set(MetricServiceDriverConf.METRIC_RECEIVER_IMPL, ETDolphinMetricReceiver.class)
        .build();

    final Configuration driverNetworkConf = NetworkConfProvider.getDriverConfiguration(DriverSideMsgHandler.class);

    final ConfigurationSerializer confSerializer = new AvroConfigurationSerializer();

    return Configurations.merge(driverConf, etMasterConfiguration, metricServiceConf,
        driverNetworkConf, getNCSConfiguration(),
        Tang.Factory.getTang().newConfigurationBuilder()
            .bindNamedParameter(SerializedServerConf.class, confSerializer.toString(serverConf))
            .bindNamedParameter(SerializedWorkerConf.class, confSerializer.toString(workerConf))
            .bindNamedParameter(SerializedParamConf.class, confSerializer.toString(userParamConf))
            .build());
  }

  private static Configuration getNCSConfiguration() {
    final Configuration nameServerConfiguration = NameServerConfiguration.CONF.build();
    final Configuration nameClientConfiguration = LocalNameResolverConfiguration.CONF.build();
    final Configuration idFactoryImplConfiguration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
        .build();

    return Configurations.merge(nameServerConfiguration, nameClientConfiguration, idFactoryImplConfiguration);
  }

  private static String processInputDir(final String inputDir, final boolean onLocal) throws InjectionException {
    if (!onLocal) {
      return inputDir;
    }
    final File inputFile = new File(inputDir);
    return "file:///" + inputFile.getAbsolutePath();
  }
}
