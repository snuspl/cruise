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
package edu.snu.spl.cruise.pregel;


import edu.snu.spl.cruise.common.centcomm.CentCommConf;
import edu.snu.spl.cruise.common.param.Parameters.*;
import edu.snu.spl.cruise.pregel.combiner.MessageCombiner;
import edu.snu.spl.cruise.pregel.graph.api.Computation;
import edu.snu.spl.cruise.pregel.PregelParameters.*;
import edu.snu.spl.cruise.services.et.configuration.ETDriverConfiguration;
import edu.snu.spl.cruise.services.et.configuration.parameters.chkp.ChkpCommitPath;
import edu.snu.spl.cruise.services.et.configuration.parameters.chkp.ChkpTempPath;
import edu.snu.spl.cruise.services.et.evaluator.api.DataParser;
import edu.snu.spl.cruise.services.et.examples.tableaccess.JobMessageLogger;
import edu.snu.spl.cruise.utils.ConfigurationUtils;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.client.parameters.JobMessageHandler;
import org.apache.reef.io.network.naming.LocalNameResolverConfiguration;
import org.apache.reef.io.network.naming.NameServerConfiguration;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.wake.IdentifierFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Launcher for Pregel application.
 */
@ClientSide
public final class PregelLauncher {
  private static final Logger LOG = Logger.getLogger(PregelLauncher.class.getName());

  /**
   * Should not be instantiated.
   */
  private PregelLauncher() {
  }

  private static List<Configuration> parseCommandLine(final String[] args,
                                                      final List<Class<? extends Name<?>>> userParamList)
      throws IOException {

    final List<Class<? extends Name<?>>> clientParamList = Arrays.asList(
        OnLocal.class, LocalRuntimeMaxNumEvaluators.class, JVMHeapSlack.class, DriverMemory.class, Timeout.class,
        ChkpCommitPath.class, ChkpTempPath.class);

    final List<Class<? extends Name<?>>> driverParamList = Arrays.asList(
        InputDir.class, NumWorkers.class, WorkerMemSize.class, WorkerNumCores.class);

    final CommandLine cl = new CommandLine();
    clientParamList.forEach(cl::registerShortNameOfClass);
    driverParamList.forEach(cl::registerShortNameOfClass);
    userParamList.forEach(cl::registerShortNameOfClass);

    final Configuration commandLineConf = cl.processCommandLine(args).getBuilder().build();
    final Configuration clientConf = ConfigurationUtils.extractParameterConf(clientParamList, commandLineConf);
    final Configuration driverConf = ConfigurationUtils.extractParameterConf(driverParamList, commandLineConf);
    final Configuration userConf = ConfigurationUtils.extractParameterConf(userParamList, commandLineConf);

    return Arrays.asList(clientConf, driverConf, userConf);
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

  private static Configuration getMasterConf(final PregelConfiguration pregelConf) throws InjectionException {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(DataParser.class, pregelConf.getDataParserClass())
        .bindNamedParameter(VertexValueCodec.class, pregelConf.getVertexValueCodecClass())
        .bindNamedParameter(EdgeCodec.class, pregelConf.getEdgeCodecClass())
        .bindNamedParameter(MessageValueCodec.class, pregelConf.getMessageValueCodecClass())
        .build();
  }

  private static Configuration getNCSConfiguration() {
    final Configuration nameServerConfiguration = NameServerConfiguration.CONF.build();
    final Configuration nameClientConfiguration = LocalNameResolverConfiguration.CONF.build();
    final Configuration idFactoryImplConfiguration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
        .build();

    final Configuration centCommConfiguration = CentCommConf.newBuilder()
        .addCentCommClient(PregelDriver.CENTCOMM_CLIENT_ID,
            DriverSideMsgHandler.class, WorkerMsgManager.class)
        .build()
        .getDriverConfiguration();

    return Configurations.merge(nameServerConfiguration, nameClientConfiguration, idFactoryImplConfiguration,
        centCommConfiguration);
  }

  public static LauncherStatus launch(final String jobName, final String[] args, final PregelConfiguration pregelConf)
      throws InjectionException, IOException {
    LauncherStatus status;

    try {
      final List<Configuration> configurations = parseCommandLine(args, pregelConf.getUserParamList());

      final Configuration clientParamConf = configurations.get(0);
      final Configuration driverParamConf = configurations.get(1);
      final Configuration userParamConf = configurations.get(2);

      final Injector clientParameterInjector = Tang.Factory.getTang().newInjector(clientParamConf);
      // runtime configuration
      final boolean onLocal = clientParameterInjector.getNamedInstance(OnLocal.class);
      final Configuration runTimeConf = onLocal ?
          getLocalRuntimeConfiguration(
              clientParameterInjector.getNamedInstance(LocalRuntimeMaxNumEvaluators.class),
              clientParameterInjector.getNamedInstance(JVMHeapSlack.class)) :
          getYarnRuntimeConfiguration(clientParameterInjector.getNamedInstance(JVMHeapSlack.class));

      final Configuration taskConf = Configurations.merge(userParamConf,
          Tang.Factory.getTang().newConfigurationBuilder()
              .bindImplementation(Computation.class, pregelConf.getComputationClass())
              .bindImplementation(MessageCombiner.class, pregelConf.getMessageCombinerClass())
              .build());

      // driver configuration
      final Configuration driverConf = getDriverConfiguration(jobName,
          clientParameterInjector.getNamedInstance(DriverMemory.class),
          clientParameterInjector.getNamedInstance(ChkpCommitPath.class),
          clientParameterInjector.getNamedInstance(ChkpTempPath.class),
          taskConf);

      final Configuration masterConf = getMasterConf(pregelConf);

      final int timeout = clientParameterInjector.getNamedInstance(Timeout.class);

      final Configuration customClientConf = Tang.Factory.getTang().newConfigurationBuilder()
          .bindNamedParameter(JobMessageHandler.class, JobMessageLogger.class)
          .build();

      status = DriverLauncher.getLauncher(Configurations.merge(runTimeConf, customClientConf))
          .run(Configurations.merge(driverConf, driverParamConf, masterConf), timeout);

    } catch (final Exception e) {
      status = LauncherStatus.failed(e);
      // This log is for giving more detailed info about failure, which status object does not show
      LOG.log(Level.WARNING, "Exception occurred", e);
    }

    LOG.log(Level.INFO, "REEF job completed: {0}", status);
    return status;
  }

  private static Configuration getDriverConfiguration(final String jobName,
                                                      final int driverMemSize,
                                                      final String chkpCommitPath,
                                                      final String chkpTempPath,
                                                      final Configuration taskConf) {
    final Configuration driverConf = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(PregelDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, jobName)
        .set(DriverConfiguration.DRIVER_MEMORY, driverMemSize)
        .set(DriverConfiguration.ON_DRIVER_STARTED, PregelDriver.StartHandler.class)
        .build();

    final Configuration etMasterConfiguration = ETDriverConfiguration.CONF
        .set(ETDriverConfiguration.CHKP_COMMIT_PATH, chkpCommitPath)
        .set(ETDriverConfiguration.CHKP_TEMP_PATH, chkpTempPath)
        .build();

    return Configurations.merge(driverConf, etMasterConfiguration, getNCSConfiguration(),
        Tang.Factory.getTang().newConfigurationBuilder()
            .bindNamedParameter(SerializedTaskletConf.class, Configurations.toString(taskConf))
            .build());
  }
}
