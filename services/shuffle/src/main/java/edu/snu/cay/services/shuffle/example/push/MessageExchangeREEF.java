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
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.util.EnvironmentUtils;

import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Simple message exchanging example using shuffle service.
 *
 * n tasks exchange tuples using the key grouping strategy. Because all tuples that are sent from one task to
 * another task are chunked into one network message, each task is blocked until exactly n messages arrive from
 * n tasks including itself.
 * (Tasks send at least an empty network message to wake up the other tasks)
 */
public final class MessageExchangeREEF {

  private static final Logger LOG = Logger.getLogger(MessageExchangeREEF.class.getName());

  /**
   * The upper limit on the number of Evaluators that the local resourcemanager will hand out concurrently.
   */
  private static final int MAX_NUMBER_OF_EVALUATORS = 15;

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
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, MessageExchangeDriver.AllocatedHandler.class)
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
    commandLine.registerShortNameOfClass(Local.class);
    final Configuration commandLineConfiguration = commandLine.parseToConfiguration(
        args, Local.class, SenderNumber.class, ReceiverNumber.class, SenderReceiverNumber.class, Timeout.class);
    final Injector injector = Tang.Factory.getTang().newInjector(commandLineConfiguration);

    final boolean isLocal = injector.getNamedInstance(Local.class);
    final long jobTimeout = injector.getNamedInstance(Timeout.class);

    final LauncherStatus state = DriverLauncher.getLauncher(getRuntimeConfiguration(isLocal))
        .run(getDriverConfiguration(commandLineConfiguration), jobTimeout);
    LOG.log(Level.INFO, "REEF job completed: {0}", state);
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private MessageExchangeREEF() {
  }

  /**
   * Command line parameter = true to run locally, or false to run on YARN.
   */
  @NamedParameter(short_name = "local", default_value = "true")
  public static final class Local implements Name<Boolean> {
  }

  /**
   * Number of tasks that works as sender.
   */
  @NamedParameter(short_name = "sender_num", default_value = "3")
  public static final class SenderNumber implements Name<Integer> {
  }

  /**
   * Number of tasks that works as receiver.
   */
  @NamedParameter(short_name = "receiver_num", default_value = "3")
  public static final class ReceiverNumber implements Name<Integer> {
  }

  /**
   * Number of tasks that works as both sender and receiver.
   */
  @NamedParameter(short_name = "sender_receiver_num", default_value = "3")
  public static final class SenderReceiverNumber implements Name<Integer> {
  }

  /**
   * Number of milliseconds to wait for the job to complete.
   */
  @NamedParameter(short_name = "timeout", default_value = "60000")
  public static final class Timeout implements Name<Long> {
  }
}
