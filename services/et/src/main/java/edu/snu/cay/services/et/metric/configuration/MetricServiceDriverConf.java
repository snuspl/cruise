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

import edu.snu.cay.services.et.driver.api.MetricReceiver;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.OptionalImpl;

/**
 * Assists to build the driver-side configuration for metric collection service.
 */
@ClientSide
public final class MetricServiceDriverConf extends ConfigurationModuleBuilder {
  public static final OptionalImpl<MetricReceiver> METRIC_RECEIVER_IMPL = new OptionalImpl<>();

  public static final ConfigurationModule CONF = new MetricServiceDriverConf()
      .bindImplementation(MetricReceiver.class, METRIC_RECEIVER_IMPL)
      .build();
}
