/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.cay.services.et.metric.configuration;

import edu.snu.cay.services.et.metric.configuration.parameter.CustomMetricCodec;
import edu.snu.cay.services.et.metric.configuration.parameter.MetricFlushPeriodMs;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.ConfigurationBuilder;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;

/**
 * Assists to build the executor-side configuration for metric collection service.
 */
@DriverSide
public final class MetricServiceExecutorConf {
  /**
   * A ConfigurationBuilder is maintained internally to bind optional parameters.
   */
  private final ConfigurationBuilder confBuilder;

  private MetricServiceExecutorConf(final ConfigurationBuilder confBuilder) {
    this.confBuilder = confBuilder;
  }

  /**
   * @return a builder object to build configuration.
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Get the configuration with the parameters bound.
   * Note that this method is for internal use only.
   */
  @Private
  public Configuration getConfiguration() {
    return confBuilder.build();
  }

  public static final class Builder implements org.apache.reef.util.Builder<MetricServiceExecutorConf> {
    private final JavaConfigurationBuilder innerBuilder = Tang.Factory.getTang().newConfigurationBuilder();

    private Builder() {
    }

    public Builder setCustomMetricCodec(final Class<? extends Codec> customMetricCodec) {
      this.innerBuilder.bindNamedParameter(CustomMetricCodec.class, customMetricCodec);
      return this;
    }

    public Builder setMetricFlushPeriodMs(final long metricFlushPeriodMs) {
      this.innerBuilder.bindNamedParameter(MetricFlushPeriodMs.class, Long.toString(metricFlushPeriodMs));
      return this;
    }

    @Override
    public MetricServiceExecutorConf build() {
      return new MetricServiceExecutorConf(innerBuilder);
    }
  }
}
