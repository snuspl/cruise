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
package edu.snu.cay.dolphin.async;

import edu.snu.cay.dolphin.async.metric.*;
import edu.snu.cay.dolphin.async.optimizer.parameters.DelayAfterOptimizationMs;
import edu.snu.cay.dolphin.async.optimizer.parameters.MetricWeightFactor;
import edu.snu.cay.dolphin.async.optimizer.parameters.MovingAverageWindowSize;
import edu.snu.cay.dolphin.async.optimizer.parameters.OptimizationIntervalMs;
import edu.snu.cay.common.aggregation.AggregationConfiguration;
import edu.snu.cay.common.param.Parameters.*;
import edu.snu.cay.common.dataloader.DataLoadingRequestBuilder;
import edu.snu.cay.services.em.driver.ElasticMemoryConfiguration;
import edu.snu.cay.services.em.optimizer.api.Optimizer;
import edu.snu.cay.services.em.optimizer.conf.OptimizerClass;
import edu.snu.cay.services.em.plan.api.PlanExecutor;
import edu.snu.cay.services.em.plan.conf.PlanExecutorClass;
import edu.snu.cay.services.em.serialize.Serializer;
import edu.snu.cay.services.ps.PSConfigurationBuilder;
import edu.snu.cay.services.ps.common.parameters.Dynamic;
import edu.snu.cay.services.ps.common.parameters.NumPartitions;
import edu.snu.cay.services.ps.common.parameters.NumServers;
import edu.snu.cay.services.ps.driver.impl.ClockManager;
import edu.snu.cay.services.ps.driver.impl.PSDriver;
import edu.snu.cay.services.ps.driver.api.PSManager;
import edu.snu.cay.services.ps.driver.impl.dynamic.DynamicPSManager;
import edu.snu.cay.services.ps.driver.impl.fixed.StaticPSManager;
import edu.snu.cay.services.ps.metric.ServerConstants;
import edu.snu.cay.services.ps.server.parameters.ServerMetricsWindowMs;
import edu.snu.cay.services.ps.server.parameters.ServerNumThreads;
import edu.snu.cay.services.ps.server.parameters.ServerQueueSize;
import edu.snu.cay.services.ps.server.parameters.ServerLogPeriod;
import edu.snu.cay.services.ps.worker.impl.AsyncWorkerClock;
import edu.snu.cay.services.ps.worker.impl.SSPWorkerClock;
import edu.snu.cay.services.ps.worker.parameters.*;
import edu.snu.cay.utils.trace.HTraceParameters;
import edu.snu.cay.utils.trace.parameters.ReceiverHost;
import edu.snu.cay.utils.trace.parameters.ReceiverPort;
import edu.snu.cay.utils.trace.parameters.ReceiverType;
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
import org.apache.reef.tang.types.NamedParameterNode;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.impl.Tuple2;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

/**
 * Main entry point for launching a {@code dolphin-async} application.
 * See {@link AsyncDolphinLauncher#launch(String, String[], AsyncDolphinConfiguration)}.
 */
@ClientSide
public final class AsyncDolphinLauncher {
  private static final Logger LOG = Logger.getLogger(AsyncDolphinLauncher.class.getName());
  private static final String DASHBOARD_DIR = "/dashboard";
  private static final String DASHBOARD_SCRIPT = "dashboard.py";

  @NamedParameter(doc = "configuration for parameters, serialized as a string")
  final class SerializedParameterConfiguration implements Name<String> {
  }

  @NamedParameter(doc = "configuration for worker class, serialized as a string")
  final class SerializedWorkerConfiguration implements Name<String> {
  }

  @NamedParameter(doc = "configuration for server class, serialized as a string")
  final class SerializedServerConfiguration implements Name<String> {
  }

  @NamedParameter(doc = "configuration for the EM worker-side client, specifically the Serializer class")
  final class SerializedEMWorkerClientConfiguration implements Name<String> {
  }

  @NamedParameter(doc = "configuration for the EM server-side client, specifically the Serializer class")
  final class SerializedEMServerClientConfiguration implements Name<String> {
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
      // parse command line arguments, separate them into basic & user parameters
      final Tuple2<Configuration, Configuration> configurations
          = parseCommandLine(args, asyncDolphinConfiguration.getParameterClassList());
      final Configuration basicParameterConf = configurations.getT1();
      final Configuration userParameterConf = configurations.getT2();
      final Injector basicParameterInjector = Tang.Factory.getTang().newInjector(basicParameterConf);

      // local or yarn runtime
      final boolean onLocal = basicParameterInjector.getNamedInstance(OnLocal.class);
      final Configuration runTimeConf = onLocal ?
          getLocalRuntimeConfiguration(
              basicParameterInjector.getNamedInstance(LocalRuntimeMaxNumEvaluators.class),
              basicParameterInjector.getNamedInstance(JVMHeapSlack.class)) :
          getYarnRuntimeConfiguration(basicParameterInjector.getNamedInstance(JVMHeapSlack.class));

      // configuration for the parameter server
      final boolean dynamic = basicParameterInjector.getNamedInstance(Dynamic.class);
      final Class<? extends PSManager> managerClass = dynamic ?
          DynamicPSManager.class :
          StaticPSManager.class;

      final Configuration parameterServerConf = PSConfigurationBuilder.newBuilder()
          .setManagerClass(managerClass)
          .setUpdaterClass(asyncDolphinConfiguration.getUpdaterClass())
          .setKeyCodecClass(asyncDolphinConfiguration.getKeyCodecClass())
          .setPreValueCodecClass(asyncDolphinConfiguration.getPreValueCodecClass())
          .setValueCodecClass(asyncDolphinConfiguration.getValueCodecClass())
          .build();

      final ConfigurationSerializer confSerializer = new AvroConfigurationSerializer();
      final Configuration serializedServerConf = Tang.Factory.getTang().newConfigurationBuilder()
          .bindNamedParameter(SerializedServerConfiguration.class,
              confSerializer.toString(asyncDolphinConfiguration.getServerConfiguration()))
          .build();

      // worker-specific configurations
      // pass the worker class implementation as well as user-defined parameters
      final Configuration basicWorkerConf = Tang.Factory.getTang().newConfigurationBuilder()
          .bindImplementation(Trainer.class, asyncDolphinConfiguration.getTrainerClass())
          .bindNamedParameter(Iterations.class,
              Integer.toString(basicParameterInjector.getNamedInstance(Iterations.class)))
          .bindNamedParameter(MiniBatches.class,
              Integer.toString(basicParameterInjector.getNamedInstance(MiniBatches.class)))
          .build();
      final Configuration workerConf = Configurations.merge(basicWorkerConf,
          asyncDolphinConfiguration.getWorkerConfiguration());

      final Configuration serializedWorkerConf = Tang.Factory.getTang().newConfigurationBuilder()
          .bindNamedParameter(SerializedWorkerConfiguration.class, confSerializer.toString(workerConf))
          .bindNamedParameter(SerializedParameterConfiguration.class, confSerializer.toString(userParameterConf))
          .build();

      final Configuration emWorkerClientConf = Tang.Factory.getTang().newConfigurationBuilder()
          .bindImplementation(Serializer.class, asyncDolphinConfiguration.getWorkerSerializerClass())
          .build();
      final Configuration emServerClientConf = Tang.Factory.getTang().newConfigurationBuilder()
          .bindImplementation(Serializer.class, asyncDolphinConfiguration.getServerSerializerClass())
          .build();

      final Configuration serializedEMClientConf = Tang.Factory.getTang().newConfigurationBuilder()
          .bindNamedParameter(SerializedEMWorkerClientConfiguration.class, confSerializer.toString(emWorkerClientConf))
          .bindNamedParameter(SerializedEMServerClientConfiguration.class, confSerializer.toString(emServerClientConf))
          .build();

      // driver-side configurations
      final Configuration driverConf = getDriverConfiguration(jobName, basicParameterInjector);
      final int timeout = basicParameterInjector.getNamedInstance(Timeout.class);

      // run dashboard and add configuration
      final JavaConfigurationBuilder dashboardConfBuilder = Tang.Factory.getTang().newConfigurationBuilder();
      final int port = basicParameterInjector.getNamedInstance(DashboardPort.class);
      try {
        if (port > 0) {
          final String hostAddress = getHostAddress(port);
          runDashboardServer(port);
          LOG.log(Level.INFO, "Add dashboard configuration!");
          dashboardConfBuilder
              .bindNamedParameter(DashboardHostAddress.class, hostAddress);
        }
      } catch (IOException e) {
        LOG.log(Level.WARNING, "Launching dashboard has failed", e);
      } finally {
        final Configuration dashboardConf = dashboardConfBuilder.build();
        final LauncherStatus status = DriverLauncher.getLauncher(runTimeConf).run(
            Configurations.merge(basicParameterConf, parameterServerConf, serializedServerConf,
                serializedWorkerConf, driverConf, customDriverConfiguration, serializedEMClientConf,
                dashboardConf),
            timeout);
        LOG.log(Level.INFO, "REEF job completed: {0}", status);
        return status;
      }
    } catch (final Exception e) {
      final LauncherStatus status = LauncherStatus.failed(e);
      LOG.log(Level.INFO, "REEF job completed: {0}", status);

      // This log is for giving more detailed info about failure, which status object does not show
      LOG.log(Level.WARNING, "Exception occurred", e);

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

  private static Tuple2<Configuration, Configuration> parseCommandLine(
      final String[] args,
      final List<Class<? extends Name<?>>> userParameterClassList) throws IOException {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    final CommandLine cl = new CommandLine(cb);

    // add all basic parameters
    // TODO #681: Need to add configuration for numWorkerThreads after multi-thread worker is enabled
    final List<Class<? extends Name<?>>> basicParameterClassList = new LinkedList<>();
    basicParameterClassList.add(EvaluatorSize.class);
    basicParameterClassList.add(InputDir.class);
    basicParameterClassList.add(OnLocal.class);
    basicParameterClassList.add(Splits.class);
    basicParameterClassList.add(Timeout.class);
    basicParameterClassList.add(LocalRuntimeMaxNumEvaluators.class);
    basicParameterClassList.add(Iterations.class);
    basicParameterClassList.add(JVMHeapSlack.class);
    basicParameterClassList.add(MiniBatches.class);
    basicParameterClassList.add(DashboardPort.class);
    basicParameterClassList.add(OptimizationBenefitThreshold.class);

    // add ps parameters
    basicParameterClassList.add(NumServers.class);
    basicParameterClassList.add(NumPartitions.class);
    basicParameterClassList.add(ServerNumThreads.class);
    basicParameterClassList.add(ServerQueueSize.class);
    basicParameterClassList.add(ServerLogPeriod.class);
    basicParameterClassList.add(ParameterWorkerNumThreads.class);
    basicParameterClassList.add(WorkerQueueSize.class);
    basicParameterClassList.add(WorkerExpireTimeout.class);
    basicParameterClassList.add(PullRetryTimeoutMs.class);
    basicParameterClassList.add(WorkerKeyCacheSize.class);
    basicParameterClassList.add(WorkerLogPeriod.class);
    basicParameterClassList.add(Dynamic.class);
    basicParameterClassList.add(ServerMetricsWindowMs.class);

    // add SSP parameters
    basicParameterClassList.add(StalenessBound.class);

    // add em parameters
    basicParameterClassList.add(OptimizerClass.class);
    basicParameterClassList.add(PlanExecutorClass.class);

    // add trace parameters
    basicParameterClassList.add(ReceiverType.class);
    basicParameterClassList.add(ReceiverHost.class);
    basicParameterClassList.add(ReceiverPort.class);

    // add optimizer parameters
    basicParameterClassList.add(OptimizationIntervalMs.class);
    basicParameterClassList.add(DelayAfterOptimizationMs.class);
    basicParameterClassList.add(MetricWeightFactor.class);
    basicParameterClassList.add(MovingAverageWindowSize.class);

    for (final Class<? extends Name<?>> basicParameterClass : basicParameterClassList) {
      cl.registerShortNameOfClass(basicParameterClass);
    }

    // also add any additional parameters specified by user
    for (final Class<? extends Name<?>> userParameterClass : userParameterClassList) {
      cl.registerShortNameOfClass(userParameterClass);
    }

    cl.processCommandLine(args);
    final Configuration clConf = cb.build();

    return new Tuple2<>(extractParameterConf(basicParameterClassList, clConf),
        extractParameterConf(userParameterClassList, clConf));
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

  private static Configuration getDriverConfiguration(
      final String jobName, final Injector injector) throws InjectionException {
    final ConfigurationModule driverConf = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(AsyncDolphinDriver.class))
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(TextInputFormat.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, jobName)
        .set(DriverConfiguration.ON_DRIVER_STARTED, AsyncDolphinDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, AsyncDolphinDriver.AllocatedEvaluatorHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_FAILED, AsyncDolphinDriver.FailedEvaluatorHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_COMPLETED, AsyncDolphinDriver.CompletedEvaluatorHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, AsyncDolphinDriver.ActiveContextHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_FAILED, AsyncDolphinDriver.FailedContextHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_CLOSED, AsyncDolphinDriver.ClosedContextHandler.class)
        .set(DriverConfiguration.ON_TASK_RUNNING, AsyncDolphinDriver.RunningTaskHandler.class)
        .set(DriverConfiguration.ON_TASK_COMPLETED, AsyncDolphinDriver.CompletedTaskHandler.class)
        .set(DriverConfiguration.ON_TASK_FAILED, AsyncDolphinDriver.FailedTaskHandler.class);

    final Configuration driverConfWithDataLoad = new DataLoadingRequestBuilder()
        .setInputFormatClass(TextInputFormat.class)
        .setInputPath(processInputDir(injector.getNamedInstance(InputDir.class), injector))
        .setNumberOfDesiredSplits(injector.getNamedInstance(Splits.class))
        .setDriverConfigurationModule(driverConf)
        .build();

    final int stalenessBound = injector.getNamedInstance(StalenessBound.class);
    final boolean isSSPModel = stalenessBound >= 0;
    final AggregationConfiguration aggregationServiceConf = isSSPModel ?
        getAggregationConfigurationForSSP() : getAggregationConfigurationDefault();
    // set up an optimizer configuration
    final Class<? extends Optimizer> optimizerClass;
    final Class<? extends PlanExecutor> executorClass;
    try {
      optimizerClass = (Class<? extends Optimizer>) Class.forName(injector.getNamedInstance(OptimizerClass.class));
      executorClass = (Class<? extends PlanExecutor>) Class.forName(injector.getNamedInstance(PlanExecutorClass.class));
    } catch (final ClassNotFoundException e) {
      throw new RuntimeException("Reflection failed", e);
    }
    final Configuration optimizerConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(Optimizer.class, optimizerClass)
        .bindImplementation(PlanExecutor.class, executorClass)
        .build();

    return Configurations.merge(driverConfWithDataLoad,
        ElasticMemoryConfiguration.getDriverConfigurationWithoutRegisterDriver(),
        PSDriver.getDriverConfiguration(),
        aggregationServiceConf.getDriverConfiguration(),
        HTraceParameters.getStaticConfiguration(),
        optimizerConf,
        NameServerConfiguration.CONF.build(),
        LocalNameResolverConfiguration.CONF.build(),
        Tang.Factory.getTang().newConfigurationBuilder()
            .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
            .build());
  }

  /**
   * Find the Host address of Client machine.
   * @param port is the port number provided by user.
   * @return String of HostAddress.
   * @throws IOException when the port number is invalid or failed to find the host address.
   */
  private static String getHostAddress(final int port) throws IOException {
    String hostAddress = "";

    // Find IP address of client PC.
    final Enumeration e = NetworkInterface.getNetworkInterfaces();
    while (e.hasMoreElements()) {
      final NetworkInterface n = (NetworkInterface) e.nextElement();
      if (n.isLoopback() || n.isVirtual() || !n.isUp()) {
        continue;
      }
      final Enumeration ee = n.getInetAddresses();
      while (ee.hasMoreElements()) {
        final InetAddress i = (InetAddress) ee.nextElement();
        if (i.isLinkLocalAddress()) {
          continue;
        }
        hostAddress = i.getHostAddress();
        break;
      }
    }

    // Check if the port number is available by trying to connect a socket to given port.
    // If the connection is successful, it means the port number is already in use.
    // Only when connectionException occurs means that the port number is available.
    try {
      (new Socket(hostAddress, port)).close();
      LOG.log(Level.WARNING, "Port number already in use.");
      throw new IOException("Port number already in use.");
    } catch (ConnectException connectException) {
      LOG.log(Level.INFO, "URL found: {0}:{1}", new Object[]{hostAddress, port});
      return hostAddress;
    }
  }

  /**
   * Copy the server launching script directory to java tmpdir and run Dashboard server on localhost.
   * @param port The port number provided by user.
   * @throws IOException when failed to copy the server script or failed to make processBuilder.
   */
  private static void runDashboardServer(final int port) throws IOException {
    LOG.log(Level.INFO, "Now launch dashboard server");

    final Path tmpPath = Paths.get(System.getProperty("java.io.tmpdir"), DASHBOARD_DIR);
    // Copy the dashboard python script to /tmp
    try {
      final FileSystem fileSystem = FileSystems.newFileSystem(
          AsyncDolphinLauncher.class.getResource("").toURI(),
          Collections.<String, String>emptyMap()
      );

      final Path jarPath = fileSystem.getPath(DASHBOARD_DIR);
      Files.walkFileTree(jarPath, new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) throws IOException {
          Files.createDirectories(tmpPath.resolve(jarPath.relativize(dir).toString()));
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
          Files.copy(file, tmpPath.resolve(jarPath.relativize(file).toString()), REPLACE_EXISTING);
          return FileVisitResult.CONTINUE;
        }
      });

      // Launch server
      final String tmpScript = Paths.get(tmpPath.toString(), DASHBOARD_SCRIPT).toString();
      final ProcessBuilder pb = new ProcessBuilder("python", tmpScript, String.valueOf(port)).inheritIO();

      final Process p = pb.start();
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          p.destroy();
        }
      });
    } catch (URISyntaxException e) {
      LOG.log(Level.WARNING, "Failed to access current jar file.", e);
    }
  }

  private static AggregationConfiguration.Builder getAggregationConfigurationDefaultBuilder() {
    return AggregationConfiguration.newBuilder()
        .addAggregationClient(SynchronizationManager.AGGREGATION_CLIENT_NAME,
            SynchronizationManager.MessageHandler.class,
            WorkerSynchronizer.MessageHandler.class)
        .addAggregationClient(WorkerConstants.AGGREGATION_CLIENT_NAME,
            DriverSideMetricsMsgHandlerForWorker.class,
            EvalSideMetricsMsgHandlerForWorker.class)
        .addAggregationClient(ServerConstants.AGGREGATION_CLIENT_NAME,
            DriverSideMetricsMsgHandlerForServer.class,
            EvalSideMetricsMsgHandlerForServer.class);
  }

  private static AggregationConfiguration getAggregationConfigurationDefault() {
    return getAggregationConfigurationDefaultBuilder()
        .addAggregationClient(ClockManager.AGGREGATION_CLIENT_NAME,
            ClockManager.MessageHandler.class,
            AsyncWorkerClock.MessageHandler.class)
        .build();
  }

  private static AggregationConfiguration getAggregationConfigurationForSSP() {
    return getAggregationConfigurationDefaultBuilder()
        .addAggregationClient(ClockManager.AGGREGATION_CLIENT_NAME,
            ClockManager.MessageHandler.class,
            SSPWorkerClock.MessageHandler.class)
        .build();
  }

  private static String processInputDir(final String inputDir, final Injector injector) throws InjectionException {
    if (!injector.getNamedInstance(OnLocal.class)) {
      return inputDir;
    }
    final File inputFile = new File(inputDir);
    return "file:///" + inputFile.getAbsolutePath();
  }
}
