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
package edu.snu.cay.services.et.configuration.metric;

import edu.snu.cay.services.et.configuration.parameters.metric.CustomMetricCodec;
import edu.snu.cay.services.et.configuration.parameters.metric.MetricFlushPeriodMs;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.OptionalParameter;

/**
 * Assists to build the executor-side configuration for metric collection service.
 */
@DriverSide
public final class MetricServiceExecutorConf extends ConfigurationModuleBuilder {
  public static final OptionalParameter<StreamingCodec> CUSTOM_METRIC_CODEC = new OptionalParameter<>();
  public static final OptionalParameter<Long> METRIC_SENDING_PERIOD_MS = new OptionalParameter<>();

  public static final ConfigurationModule CONF = new MetricServiceExecutorConf()
      .bindNamedParameter(CustomMetricCodec.class, CUSTOM_METRIC_CODEC)
      .bindNamedParameter(MetricFlushPeriodMs.class, METRIC_SENDING_PERIOD_MS)
      .build();
}
