/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package edu.snu.reef.examples.elastic.memory;

import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.io.network.group.impl.driver.GroupCommService;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.util.EnvironmentUtils;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Client for the elastic memory demo app
 */
@ClientSide
public class ElasticMemoryREEF {

  private static final Logger LOG = Logger.getLogger(ElasticMemoryREEF.class.getName());

  /**
   * The upper limit on the number of Evaluators that the local resource manager will hand out concurrently
   */
  private static final int MAX_NUMBER_OF_EVALUATORS = 16;

  private static boolean isLocal;
  private static int jobTimeout;
  private static int splitNum;
  private static String inputDir;

  private static Configuration parseCommandLine(final String[] args) {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();

    try {
      new CommandLine(cb)
          .registerShortNameOfClass(Local.class)
          .registerShortNameOfClass(TimeOut.class)
          .registerShortNameOfClass(SplitNum.class)
          .registerShortNameOfClass(InputDir.class)
          .processCommandLine(args);
    } catch (IOException ex) {
      final String msg = "Unable to parse command line";
      LOG.log(Level.SEVERE, msg, ex);
      throw new RuntimeException(msg, ex);
    }
    return cb.build();
  }

  private static void storeCommandLineArgs(
      final Configuration commandLineConf) throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector(commandLineConf);
    isLocal = injector.getNamedInstance(Local.class);
    jobTimeout = injector.getNamedInstance(TimeOut.class) * 60 * 1000;
    splitNum = injector.getNamedInstance(SplitNum.class);
    inputDir = injector.getNamedInstance(InputDir.class);
  }

  private static Configuration getRuntimeConfiguration() {
    final Configuration runtimeConfiguration;
    if (isLocal) {
      LOG.log(Level.INFO, "Running Data Loading demo on the local runtime");
      runtimeConfiguration = LocalRuntimeConfiguration.CONF
          .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, MAX_NUMBER_OF_EVALUATORS)
          .build();
    } else {
      LOG.log(Level.INFO, "Running Data Loading demo on YARN");
      runtimeConfiguration = YarnClientConfiguration.CONF.build();
    }
    return runtimeConfiguration;
  }

  private static Configuration getDriverConfiguration() {
    final Configuration driverConfiguration = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(ElasticMemoryDriver.class))
//        .set(DriverConfiguration.ON_DRIVER_STARTED, ElasticMemoryDriver.StartHandler.class)
//        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, ElasticMemoryDriver.EvaluatorAllocateHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, ElasticMemoryDriver.ContextActiveHandler.class)
//        .set(DriverConfiguration.ON_CONTEXT_CLOSED, ElasticMemoryDriver.ContextCloseHandler.class)
        .set(DriverConfiguration.ON_TASK_COMPLETED, ElasticMemoryDriver.TaskCompletedHandler.class)
//        .set(DriverConfiguration.ON_TASK_FAILED, ElasticMemoryDriver.FailedTaskHandler.class)
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "ElasticMemoryREEF")
        .build();
    final Configuration groupCommServConfiguration = GroupCommService.getConfiguration();

    final Configuration mergedDriverConfiguration = Tang.Factory.getTang()
        .newConfigurationBuilder(groupCommServConfiguration, driverConfiguration)
        .build();

    return mergedDriverConfiguration;
  }

  private static LauncherStatus runElasticMemory(
      final Configuration runtimeConfiguration) throws InjectionException {
    final Configuration driverConfiguration = getDriverConfiguration();

    LOG.info(new AvroConfigurationSerializer().toString(driverConfiguration));

    return DriverLauncher.getLauncher(runtimeConfiguration).run(driverConfiguration, jobTimeout);
  }

  public static void main(final String[] args)
      throws InjectionException, BindException, IOException {

    final Configuration parsedConfig = parseCommandLine(args);

    storeCommandLineArgs(parsedConfig);

    final Configuration runtimeConfiguration = getRuntimeConfiguration();

    final LauncherStatus state = runElasticMemory(runtimeConfiguration);

    LOG.log(Level.INFO, "REEF job completed: {0}", state);
  }

  /**
   * Command line parameter = true to run locally, or false to run on YARN.
   */
  @NamedParameter(doc = "Whether or not to run on the local runtime",
      short_name = "local", default_value = "true")
  public static final class Local implements Name<Boolean> {
  }

  @NamedParameter(doc = "Number of minutes before timeout",
      short_name = "timeout", default_value = "2")
  public static final class TimeOut implements Name<Integer> {
  }

  @NamedParameter(doc = "Number of desired splits",
      short_name = "split", default_value = "5")
  public static final class SplitNum implements Name<Integer> {
  }

  @NamedParameter(short_name = "input")
  public static final class InputDir implements Name<String> {
  }
}
