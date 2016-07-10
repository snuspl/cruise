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

  /**
   * @return Configuration built by {@link Builder}. Note that this method throws a runtime exception
   * unless {@link Builder#build()} is complete.
   */
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
    private Class<? extends MetricsMsgSender> metricsMsgSenderClass = null;

    private Builder() {
    }

    /**
     * @param codecClassParam Class of the codec used to (de-)serialize the MetricsMessage
     */
    public Builder setMetricsMsgCodecClass(final Class<? extends Codec> codecClassParam) {
      this.codecClass = codecClassParam;
      return this;
    }

    /**
     * @param metricsHandlerClassParam Class for handling Metrics sent by {@link MetricsCollector}
     */
    public Builder setMetricsHandlerClass(final Class<? extends MetricsHandler> metricsHandlerClassParam) {
      this.metricsHandlerClass = metricsHandlerClassParam;
      return this;
    }

    /**
     * @param metricsMsgSenderClassParam Class for sending MetricsMessage including raw metrics and app-specific info
     */
    public Builder setMetricsMsgSenderClass(final Class<? extends MetricsMsgSender> metricsMsgSenderClassParam) {
      this.metricsMsgSenderClass = metricsMsgSenderClassParam;
      return this;
    }

    @Override
    public MetricsCollectionServiceConf build() {
      BuilderUtils.notNull(codecClass);
      BuilderUtils.notNull(metricsHandlerClass);
      BuilderUtils.notNull(metricsMsgSenderClass);
      final Configuration conf = Configurations.merge(
          ServiceConfiguration.CONF
              .set(ServiceConfiguration.SERVICES, MetricsCollector.class)
              .build(),
          Tang.Factory.getTang().newConfigurationBuilder()
              .bindImplementation(Codec.class, codecClass)
              .bindImplementation(MetricsHandler.class, metricsHandlerClass)
              .bindImplementation(MetricsMsgSender.class, metricsMsgSenderClass)
              .build()
      );
      return new MetricsCollectionServiceConf(conf);
    }
  }
}
