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
package edu.snu.cay.common.metric;

import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.util.BuilderUtils;
import org.apache.reef.wake.remote.Codec;

/**
 * Configuration for MetricsCollectionService.
 */
public final class MetricsCollectionServiceConf {
  private Configuration conf;

  private MetricsCollectionServiceConf(final Configuration conf) {
    this.conf = conf;
  }

  /**
   * Create a new Builder object to build configuration for MetricsCollection service.
   */
  public static Builder newBuilder() {
    return new MetricsCollectionServiceConf.Builder();
  }

  public Configuration getConfiguration() {
    if (conf == null) {
      throw new RuntimeException("Configuration has not been configured properly in MetricCollectionConf");
    } else {
      return conf;
    }
  }

  public static final class Builder implements org.apache.reef.util.Builder<MetricsCollectionServiceConf> {
    private Class<? extends Codec> codecClass = null;
    private Class<? extends MetricsHandler> metricsHandlerClass = null;

    private Builder() {
    }

    public Builder setMetricsMsgCodecClass(final Class<? extends Codec> codecClassParam) {
      this.codecClass = codecClassParam;
      return this;
    }

    public Builder setMetricsHandlerClass(final Class<? extends MetricsHandler> metricsHandlerClassParam) {
      this.metricsHandlerClass = metricsHandlerClassParam;
      return this;
    }

    @Override
    public MetricsCollectionServiceConf build() {
      BuilderUtils.notNull(codecClass);
      BuilderUtils.notNull(metricsHandlerClass);
      final Configuration conf = Configurations.merge(
          ServiceConfiguration.CONF
              .set(ServiceConfiguration.SERVICES, MetricsCollector.class)
              .build(),
          Tang.Factory.getTang().newConfigurationBuilder()
              .bindImplementation(Codec.class, codecClass)
              .bindImplementation(MetricsHandler.class, metricsHandlerClass)
              .build()
      );
      return new MetricsCollectionServiceConf(conf);
    }
  }
}
