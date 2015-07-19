/**
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

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;

/**
 * Metric tracker for the metrics that are defined by user.
 */
public class UserMetricTracker implements MetricTracker {
  /**
   * Metric collected after start() and before stop().
   */
  private final Map<String, Double> metrics = new HashMap<>();

  @Inject
  public UserMetricTracker() {
  }

  @Override
  public void start() {
    metrics.clear();
  }

  @Override
  public Map<String, Double> stop() {
    return metrics;
  }

  @Override
  public void close() throws Exception {
  }

  /**
   * Add record to the metric.
   * TODO Metric can only be recorded at most once inside an interval
   * @param key identifier to distinguish the metric
   * @param value metric value to record
   * @throws MetricException When the metric is already recorded with the given key
   */
  public void record(final String key, final double value) throws MetricException {
    if (metrics.containsKey(key)) {
      final String msg = new StringBuilder()
              .append("The metric was already recorded with key ")
              .append(key)
              .append(" in this interval.")
              .toString();
      throw new MetricException(msg);
    } else {
      metrics.put(key, value);
    }
  }
}
