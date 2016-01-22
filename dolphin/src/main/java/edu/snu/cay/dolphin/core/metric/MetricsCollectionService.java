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
package edu.snu.cay.dolphin.core.metric;

import edu.snu.cay.dolphin.core.metric.ns.MetricsMessageHandler;
import edu.snu.cay.dolphin.core.metric.ns.NetworkContextRegister;
import edu.snu.cay.dolphin.core.metric.ns.NetworkDriverRegister;
import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.driver.parameters.DriverStartHandler;
import org.apache.reef.evaluator.context.parameters.ContextStartHandlers;
import org.apache.reef.evaluator.context.parameters.ContextStopHandlers;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Unit;

import javax.inject.Inject;

/**
 * Provides configurations for the service that collects metrics from Tasks.
 * Should be inserted alongside a context.
 */
@Unit
public final class MetricsCollectionService {

  @Inject
  private MetricsCollectionService() {
  }

  /**
   * Configuration for REEF driver when using {@link MetricsMessageSender} for metric sender.
   * Binds NetworkConnectionService registration handlers and MetricsMessage codec/handler.
   * @return configuration that should be submitted with a DriverConfiguration
   */
  public static Configuration getDriverConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindSetEntry(DriverStartHandler.class, NetworkDriverRegister.RegisterDriverHandler.class)
        .bindNamedParameter(MetricsMessageHandler.class, DriverSideMetricsMsgHandler.class)
        .build();
  }

  /**
   * Binds NetworkConnectionService registration handlers.
   * @return configuration to which a NetworkConnectionService registration handlers are added
   */
  public static Configuration getContextConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindSetEntry(ContextStartHandlers.class, NetworkContextRegister.RegisterContextHandler.class)
        .bindSetEntry(ContextStopHandlers.class, NetworkContextRegister.UnregisterContextHandler.class)
        .build();
  }

  /**
   * Return the service configuration for the Metrics Collection Service.
   * @return service configuration for the Metrics Collection Service
   */
  public static Configuration getServiceConfiguration() {
    return Configurations.merge(
        ServiceConfiguration.CONF
            .set(ServiceConfiguration.SERVICES, MetricsCollector.class)
            .build(),
        Tang.Factory.getTang().newConfigurationBuilder()
            .bindImplementation(MetricsHandler.class, MetricsMessageSender.class)
            .bindNamedParameter(MetricsMessageHandler.class, EvalSideMetricsMsgHandler.class)
            .build()
    );
  }
}
