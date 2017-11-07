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
package edu.snu.cay.services.et.driver.api;

import edu.snu.cay.services.et.avro.MetricMsg;
import edu.snu.cay.services.et.driver.impl.LoggingMetricReceiver;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * Receives metrics from Executors.
 */
@DefaultImplementation(LoggingMetricReceiver.class)
public interface MetricReceiver {

  /**
   * Called when a metric msg arrives from an executor.
   * @param srcId The executor id that sent this metric.
   * @param msg The message containing metrics.
   */
  void onMetricMsg(String srcId, MetricMsg msg);
}
