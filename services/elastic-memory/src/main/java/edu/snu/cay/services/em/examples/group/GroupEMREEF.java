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
package edu.snu.cay.services.em.examples.group;

import edu.snu.cay.services.em.driver.ElasticMemoryConfiguration;
import edu.snu.cay.utils.trace.HTraceParameters;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.io.network.naming.LocalNameResolverConfiguration;
import org.apache.reef.io.network.naming.NameServerConfiguration;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.wake.IdentifierFactory;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Client code for GroupEM.
 */
public final class GroupEMREEF {
  private static final Logger LOG = Logger.getLogger(GroupEMREEF.class.getName());

  public static final int TIMEOUT = 100000;

  /**
   * Should not be instantiated.
   */
  private GroupEMREEF() {
  }

  /**
   * Get driver configuration.
   */
  public static Configuration getDriverConfiguration() throws InjectionException {
    final Configuration traceConf = Tang.Factory.getTang().newInjector().getInstance(HTraceParameters.class).
        getConfiguration();
    final Configuration driverConfiguration = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(GroupEMDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "GroupEMDriver")
        .set(DriverConfiguration.ON_DRIVER_STARTED, GroupEMDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, GroupEMDriver.AllocatedEvaluatorHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, GroupEMDriver.ActiveContextHandler.class)
        .build();
    return Configurations.merge(
        ElasticMemoryConfiguration.getDriverConfiguration(),
        traceConf,
        driverConfiguration,
        LocalNameResolverConfiguration.CONF.build(),
        NameServerConfiguration.CONF.build(),
        Tang.Factory.getTang().newConfigurationBuilder()
            .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
            .build()
    );
  }

  /**
   * Run.
   */
  public static void main(final String[] args) throws InjectionException {
    final Configuration runtimeConf = LocalRuntimeConfiguration.CONF.build();
    final LauncherStatus status = DriverLauncher.getLauncher(runtimeConf).run(getDriverConfiguration(), TIMEOUT);
    LOG.log(Level.INFO, "REEF job completed: {0}", status);
  }
}
