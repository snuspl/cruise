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
package edu.snu.cay.dolphin.core;

import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.io.data.loading.api.DataLoadingRequestBuilder;
import org.apache.reef.io.network.group.impl.driver.GroupCommService;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.util.EnvironmentUtils;

import javax.inject.Inject;
import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Job launch code for Dolphin jobs.
 */
public final class DolphinLauncher {
  private static final Logger LOG = Logger.getLogger(DolphinLauncher.class.getName());
  private final DolphinParameters dolphinParameters;

  @Inject
  private DolphinLauncher(final DolphinParameters dolphinParameters) {
    this.dolphinParameters = dolphinParameters;
  }

  public static void run(final Configuration dolphinConfig) {
    LauncherStatus status;
    try {
      status = Tang.Factory.getTang()
          .newInjector(dolphinConfig)
          .getInstance(DolphinLauncher.class)
          .run();
    } catch (final Exception e) {
      status = LauncherStatus.failed(e);
    }

    LOG.log(Level.INFO, "REEF job completed: {0}", status);
  }

  private LauncherStatus run() throws InjectionException {
    return DriverLauncher.getLauncher(getRuntimeConfiguration())
        .run(getDriverConfWithDataLoad(), dolphinParameters.getTimeout());
  }

  private Configuration getRuntimeConfiguration() {
    return dolphinParameters.getOnLocal() ? getLocalRuntimeConfiguration() : getYarnRuntimeConfiguration();
  }

  private Configuration getYarnRuntimeConfiguration() {
    return YarnClientConfiguration.CONF.build();
  }

  private Configuration getLocalRuntimeConfiguration() {
    return LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, dolphinParameters.getLocalRuntimeMaxNumEvaluators())
        .build();
  }

  private Configuration getDriverConfWithDataLoad() {
    final ConfigurationModule driverConfiguration = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(DolphinDriver.class))
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(TextInputFormat.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, dolphinParameters.getIdentifier())
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, DolphinDriver.ActiveContextHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_MESSAGE, DolphinDriver.ContextMessageHandler.class)
        .set(DriverConfiguration.ON_TASK_COMPLETED, DolphinDriver.TaskCompletedHandler.class)
        .set(DriverConfiguration.ON_TASK_RUNNING, DolphinDriver.TaskRunningHandler.class)
        .set(DriverConfiguration.ON_TASK_FAILED, DolphinDriver.TaskFailedHandler.class);

    final EvaluatorRequest evalRequest = EvaluatorRequest.newBuilder()
        .setNumber(1)
        .setMemory(dolphinParameters.getEvalSize())
        .build();

    final Configuration driverConfWithDataLoad = new DataLoadingRequestBuilder()
        .setMemoryMB(dolphinParameters.getEvalSize())
        .setInputFormatClass(TextInputFormat.class)
        .setInputPath(processInputDir(dolphinParameters.getInputDir()))
        .setNumberOfDesiredSplits(dolphinParameters.getDesiredSplits())
        .setComputeRequest(evalRequest)
        .setDriverConfigurationModule(driverConfiguration)
        .build();

    return Configurations.merge(driverConfWithDataLoad,
        GroupCommService.getConfiguration(),
        dolphinParameters.getDriverConf());
  }

  private String processInputDir(final String inputDir) {
    if (!dolphinParameters.getOnLocal()) {
      return inputDir;
    }
    final File inputFile = new File(inputDir);
    return "file:///" + inputFile.getAbsolutePath();
  }
}
