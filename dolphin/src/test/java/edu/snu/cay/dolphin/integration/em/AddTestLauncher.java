/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.cay.dolphin.integration.em;

import edu.snu.cay.dolphin.core.DolphinDriver;
import edu.snu.cay.dolphin.core.DolphinParameters;
import edu.snu.cay.dolphin.core.optimizer.OptimizationConfiguration;
import edu.snu.cay.services.dataloader.DataLoadingRequestBuilder;
import edu.snu.cay.services.em.driver.ElasticMemoryConfiguration;
import edu.snu.cay.utils.trace.HTraceParameters;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.io.data.output.TaskOutputServiceBuilder;
import org.apache.reef.io.data.output.TaskOutputStreamProvider;
import org.apache.reef.io.data.output.TaskOutputStreamProviderHDFS;
import org.apache.reef.io.data.output.TaskOutputStreamProviderLocal;
import org.apache.reef.io.network.group.impl.driver.GroupCommService;
import org.apache.reef.io.network.naming.LocalNameResolverConfiguration;
import org.apache.reef.io.network.naming.NameServerConfiguration;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.util.EnvironmentUtils;

import javax.inject.Inject;
import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Dolphin Launcher for EM add integration test.
 * Use NoOperationOptimizerConfiguration for optimizing
 * and AddTestStartHandler for driver start handler.
 */
public final class AddTestLauncher {
  private static final Logger LOG = Logger.getLogger(AddTestLauncher.class.getName());
  private final DolphinParameters dolphinParameters;
  private final HTraceParameters traceParameters;
  private final int addEvalNumber;
  private final int addThreadNumber;

  @Inject
  private AddTestLauncher(final DolphinParameters dolphinParameters,
                          final HTraceParameters traceParameters,
                          @Parameter(AddIntegrationTest.AddEvalNumber.class) final int addEvalNumber,
                          @Parameter(AddIntegrationTest.AddThreadNumber.class) final int addThreadNumber) {
    this.dolphinParameters = dolphinParameters;
    this.traceParameters = traceParameters;
    this.addEvalNumber = addEvalNumber;
    this.addThreadNumber = addThreadNumber;
  }

  public static LauncherStatus run(final Configuration dolphinConfig) {
    LauncherStatus status;
    try {
      status = Tang.Factory.getTang()
          .newInjector(dolphinConfig)
          .getInstance(AddTestLauncher.class).run();
    } catch (final Exception e) {
      status = LauncherStatus.failed(e);
    }

    LOG.log(Level.INFO, "REEF job completed: {0}", status);
    return status;
  }

  private LauncherStatus run() throws InjectionException {
    return DriverLauncher.getLauncher(getLocalRuntimeConfiguration())
        .run(getDriverConfiguration(), dolphinParameters.getTimeout());
  }

  private Configuration getLocalRuntimeConfiguration() {
    return LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, dolphinParameters.getLocalRuntimeMaxNumEvaluators())
        .build();
  }

  private Configuration getDriverConfiguration() {
    final ConfigurationModule driverConfiguration = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(DolphinDriver.class))
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(TextInputFormat.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, dolphinParameters.getIdentifier())
        .set(DriverConfiguration.ON_DRIVER_STARTED, DolphinDriver.StartHandler.class)
        .set(DriverConfiguration.ON_DRIVER_STARTED, AddTestStartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, DolphinDriver.EvaluatorAllocatedHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_FAILED, DolphinDriver.EvaluatorFailedHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, DolphinDriver.ActiveContextHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_MESSAGE, DolphinDriver.ContextMessageHandler.class)
        .set(DriverConfiguration.ON_TASK_COMPLETED, DolphinDriver.TaskCompletedHandler.class)
        .set(DriverConfiguration.ON_TASK_RUNNING, DolphinDriver.TaskRunningHandler.class)
        .set(DriverConfiguration.ON_TASK_FAILED, DolphinDriver.TaskFailedHandler.class);

    final EvaluatorRequest compRequest = EvaluatorRequest.newBuilder()
        .setNumber(1)
        .setMemory(dolphinParameters.getEvalSize())
        .build();

    // We do not explicitly set the number of data loading evaluators here, because
    // the number is being reset to the number of data partitions at the Driver in DataLoader anyway.
    final EvaluatorRequest dataRequest = EvaluatorRequest.newBuilder()
        .setMemory(dolphinParameters.getEvalSize())
        .build();

    final Configuration driverConfWithDataLoad = new DataLoadingRequestBuilder()
        .setInputFormatClass(TextInputFormat.class)
        .setInputPath(processInputDir(dolphinParameters.getInputDir()))
        .setNumberOfDesiredSplits(dolphinParameters.getDesiredSplits())
        .addComputeRequest(compRequest)
        .addDataRequest(dataRequest)
        .setDriverConfigurationModule(driverConfiguration)
        .build();

    final Configuration outputServiceConf = TaskOutputServiceBuilder.CONF
        .set(TaskOutputServiceBuilder.TASK_OUTPUT_STREAM_PROVIDER, getTaskOutputStreamProvider())
        .set(TaskOutputServiceBuilder.OUTPUT_PATH, processOutputDir(dolphinParameters.getOutputDir()))
        .build();

    final Configuration optimizerConf = OptimizationConfiguration.getNoOperationOptimizerConfiguration();

    final Configuration testConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(AddIntegrationTest.AddEvalNumber.class, Integer.toString(addEvalNumber))
        .bindNamedParameter(AddIntegrationTest.AddThreadNumber.class, Integer.toString(addThreadNumber))
        .build();

    return Configurations.merge(driverConfWithDataLoad,
        outputServiceConf,
        optimizerConf,
        traceParameters.getConfiguration(),
        GroupCommService.getConfiguration(),
        ElasticMemoryConfiguration.getDriverConfiguration(),
        NameServerConfiguration.CONF.build(),
        LocalNameResolverConfiguration.CONF.build(),
        dolphinParameters.getDriverConf(),
        testConf);
  }

  private String processInputDir(final String inputDir) {
    if (!dolphinParameters.getOnLocal()) {
      return inputDir;
    }
    final File inputFile = new File(inputDir);
    return "file:///" + inputFile.getAbsolutePath();
  }

  /**
   * If a relative local file path is given as the output directory,
   * transform the relative path into the absolute path based on the current directory where the user runs REEF.
   * @param outputDir path of the output directory given by the user
   * @return
   */
  private String processOutputDir(final String outputDir) {
    if (!dolphinParameters.getOnLocal()) {
      return outputDir;
    }
    final File outputFile = new File(outputDir);
    return outputFile.getAbsolutePath();
  }

  private Class<? extends TaskOutputStreamProvider> getTaskOutputStreamProvider() {
    return dolphinParameters.getOnLocal() ?
        TaskOutputStreamProviderLocal.class : TaskOutputStreamProviderHDFS.class;
  }
}
