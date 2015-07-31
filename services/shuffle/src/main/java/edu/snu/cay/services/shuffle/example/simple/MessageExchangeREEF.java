/**
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
package edu.snu.cay.services.shuffle.example.simple;

import edu.snu.cay.services.shuffle.driver.ShuffleDriverConfiguration;
import edu.snu.cay.services.shuffle.driver.StaticShuffleManager;
import edu.snu.cay.services.shuffle.utils.NameResolverWrapper;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
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

/**
 * Simple message exchanging example using shuffle service.
 *
 * n tasks exchange tuples using the key grouping strategy. Because all tuples that are sent from one evaluator to
 * another evaluator are chunked into one network message, each evaluator is blocked until exactly n messages arrive from
 * n tasks including itself.
 * (Tasks send at least an empty network message to wake the other tasks)
 */
public final class MessageExchangeREEF {

  public static final Logger LOG = Logger.getLogger(MessageExchangeREEF.class.getName());

  public static final int MAX_NUMBER_OF_EVALUATORS = 15;

  public static Configuration getRuntimeConfiguration(final boolean isLocal) {
    if (isLocal) {
      return LocalRuntimeConfiguration.CONF
          .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, MAX_NUMBER_OF_EVALUATORS)
          .build();
    } else {
      return YarnClientConfiguration.CONF.build();
    }
  }

  public static Configuration getDriverConfiguration(final int taskNumber) {
    final Configuration taskNumberConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(MessageExchangeREEF.TaskNumber.class, taskNumber + "")
        .bindImplementation(NameResolver.class, NameResolverWrapper.class)
        .build();

    final Configuration driverConf = DriverConfiguration.CONF
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "MessageExchangeREEF")
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(MessageExchangeDriver.class))
        .set(DriverConfiguration.ON_DRIVER_STARTED, MessageExchangeDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, MessageExchangeDriver.AllocatedHandler.class)
        .build();

    final Configuration shuffleConf = ShuffleDriverConfiguration.CONF
        .set(ShuffleDriverConfiguration.SHUFFLE_MANAGER_CLASS_NAME, StaticShuffleManager.class.getName())
        .build();

    return Configurations.merge(taskNumberConf, driverConf, shuffleConf);
  }

  public static void main(final String[] args) throws Exception {
    final CommandLine commandLine = new CommandLine();
    commandLine.registerShortNameOfClass(Local.class);
    final Injector injector = Tang.Factory.getTang().newInjector(
        commandLine.parseToConfiguration(args, Local.class, TaskNumber.class, Timeout.class));

    final boolean isLocal = injector.getNamedInstance(Local.class);
    final int taskNumber = injector.getNamedInstance(TaskNumber.class);
    final long jobTimeout = injector.getNamedInstance(Timeout.class);

    final LauncherStatus state = DriverLauncher.getLauncher(getRuntimeConfiguration(isLocal))
        .run(getDriverConfiguration(taskNumber), jobTimeout);
    LOG.log(Level.INFO, "REEF job completed: {0}", state);
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private MessageExchangeREEF() {
  }

  @NamedParameter(short_name = "local", default_value = "true")
  public static final class Local implements Name<Boolean> {
  }

  @NamedParameter(short_name = "task_num", default_value = "5")
  public static final class TaskNumber implements Name<Integer> {
  }

  @NamedParameter(short_name = "timeout", default_value = "30000")
  public static final class Timeout implements Name<Long> {
  }
}
