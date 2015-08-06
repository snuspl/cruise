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

import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.evaluator.context.parameters.ContextMessageSources;
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
   * Return the service configuration for the Metrics Collection Service.
   * @return service configuration for the Metrics Collection Service
   */
  public static Configuration getServiceConfiguration() {
    return ServiceConfiguration.CONF
        .set(ServiceConfiguration.SERVICES, MetricsCollector.class)
        .build();
  }

  /**
   * Return the context configuration for the Metrics Collection Service.
   * @return context configuration for the Metrics Collection Service
   */
  public static Configuration getContextConfiguration() {
    return ContextConfiguration.CONF
        .set(ContextConfiguration.IDENTIFIER, MetricsCollectionService.class.getName())
        .set(ContextConfiguration.ON_SEND_MESSAGE, MetricsCollector.class)
        .build();
  }

  /**
   * Add a context message source to the pre-existed context configuration.
   * @param previousConfiguration pre-existed context configuration
   * @return context configuration to which a context message source is added
   */
  public static Configuration getContextConfiguration(final Configuration previousConfiguration) {
    final Configuration contextConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindSetEntry(ContextMessageSources.class, MetricsCollector.class)
        .build();

    return Configurations.merge(contextConf, previousConfiguration);
  }
}
