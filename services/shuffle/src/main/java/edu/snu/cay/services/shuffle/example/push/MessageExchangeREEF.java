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
package edu.snu.cay.services.shuffle.example.push;

import edu.snu.cay.services.shuffle.driver.ShuffleDriverConfiguration;
import edu.snu.cay.services.shuffle.driver.impl.StaticPushShuffleManager;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.io.network.naming.LocalNameResolverImpl;
import org.apache.reef.io.network.naming.NameResolver;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.util.EnvironmentUtils;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Message exchanging example using push-based shuffle.
 *
 * SenderTasks send random number of tuples to ReceiverTasks during certain number of iterations.
 * SenederAndReceiverTasks simultaneously works as SenderTask and ReceiverTask.
 * A user can set the number of iterations.
 * Each iteration, receivers should receive tuples that are sent in the same iteration.
 *
 * If shutdown parameter is set to true, the application will be shutdown when the number of iterations reaches
 * the number that user set.
 *
 * The driver checks if total number of sent tuples and received tuples are same when all tasks are completed.
 */
public final class MessageExchangeREEF {

  private static final Logger LOG = Logger.getLogger(MessageExchangeREEF.class.getName());

  /**
   * The upper limit on the number of Evaluators that the local resourcemanager will hand out concurrently.
   */
  private static final int MAX_NUMBER_OF_EVALUATORS = 20;

  private static Configuration getRuntimeConfiguration(final boolean isLocal) {
    if (isLocal) {
      return LocalRuntimeConfiguration.CONF
          .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, MAX_NUMBER_OF_EVALUATORS)
          .build();
    } else {
      return YarnClientConfiguration.CONF.build();
    }
  }

  private static Configuration getDriverConfiguration(final Configuration baseConf) {
    final Configuration driverConf = DriverConfiguration.CONF
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "MessageExchangeREEF")
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(MessageExchangeDriver.class))
        .set(DriverConfiguration.ON_DRIVER_STARTED, MessageExchangeDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, MessageExchangeDriver.EvaluatorAllocatedHandler.class)
        .set(DriverConfiguration.ON_TASK_COMPLETED, MessageExchangeDriver.TaskCompletedHandler.class)
        .build();

    final Configuration shuffleConf = ShuffleDriverConfiguration.CONF
        .set(ShuffleDriverConfiguration.SHUFFLE_MANAGER_CLASS_NAME, StaticPushShuffleManager.class.getName())
        .build();

    final Configuration networkConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(NameResolver.class, LocalNameResolverImpl.class)
        .build();

    return Configurations.merge(baseConf, driverConf, networkConf, shuffleConf);
  }

  /**
   * Start MessageExchangeREEF job.
   *
   * @param args command line parameters.
   * @throws Exception an unexpected exception
   */
  public static void main(final String[] args) throws Exception {
    final CommandLine commandLine = new CommandLine();
    commandLine.registerShortNameOfClass(MessageExchangeParameters.Local.class);

    final Configuration commandLineConfiguration = commandLine.parseToConfiguration(
        args, MessageExchangeParameters.Local.class, MessageExchangeParameters.SenderNumber.class,
        MessageExchangeParameters.ReceiverNumber.class, MessageExchangeParameters.Timeout.class,
        MessageExchangeParameters.Shutdown.class, MessageExchangeParameters.TotalIterationNum.class,
        MessageExchangeParameters.ShutdownIterationNum.class);

    final Injector injector = Tang.Factory.getTang().newInjector(commandLineConfiguration);

    final boolean isLocal = injector.getNamedInstance(MessageExchangeParameters.Local.class);
    final long jobTimeout = injector.getNamedInstance(MessageExchangeParameters.Timeout.class);

    final LauncherStatus state = DriverLauncher.getLauncher(getRuntimeConfiguration(isLocal))
        .run(getDriverConfiguration(commandLineConfiguration), jobTimeout);
    LOG.log(Level.INFO, "REEF job completed: {0}", state);
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private MessageExchangeREEF() {
  }
}
