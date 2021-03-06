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
package edu.snu.spl.cruise.common.metric;

import edu.snu.spl.cruise.common.metric.avro.Metrics;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.wake.EventHandler;

/**
 * Receives Metrics from MetricsCollector, and passes the collected Metrics
 * to the one who wants to handle them. For example, Tasks may want to include
 * information about iteration, number of data, when sending Metrics.
 */
@EvaluatorSide
public interface MetricsHandler extends EventHandler<Metrics> {

  /**
   * Receives the Metrics, which consist of mapping between {@code String} type keys and {@code Double} type values.
   */
  void onNext(Metrics metrics);

  /**
   * @return Metrics received in {@link #onNext(Metrics)}.
   */
  Metrics getMetrics();
}
