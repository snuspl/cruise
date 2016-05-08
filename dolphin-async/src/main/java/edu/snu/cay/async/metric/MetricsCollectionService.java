/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.cay.async.metric;

import edu.snu.cay.common.metric.MetricsCollector;
import edu.snu.cay.common.metric.MetricsHandler;
import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;

/**
 * Provides configurations for the service that collects metrics from Tasks.
 */
public final class MetricsCollectionService {
  public static final String AGGREGATION_CLIENT_NAME = MetricsCollectionService.class.getName();

  private MetricsCollectionService() {
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
            .build()
    );
  }
}
