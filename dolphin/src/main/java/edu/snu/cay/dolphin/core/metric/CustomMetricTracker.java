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
 * Metric tracker for custom values.
 * Each metric is identified with the key, which is given from setMetric(key, value).
 *
 * This class is thread-safe because multiple threads can access it simultaneously.
 */
public class CustomMetricTracker implements MetricTracker {

  /**
   * The metrics identified by key
   */
  private final Map<String, Double> metrics = new HashMap<>();

  /**
   * Indicates that the intervals are ready to record.
   */
  private boolean isStarted = false;

  @Inject
  private CustomMetricTracker() {
  }

  @Override
  public synchronized void start() {
    isStarted = true;
  }

  @Override
  public synchronized Map<String, Double> stop() {
    final Map<String, Double> result = new HashMap<>();
    result.putAll(metrics);

    isStarted = false;
    metrics.clear();

    return result;
  }

  @Override
  public void close() throws Exception {
    // Do nothing.
  }

  /**
   * Set the metric with the given key and value
   * @param key identifier to distinguish the metric
   * @throws MetricException if the metricTracker is not between start() and stop(),
   */
  public synchronized void setMetric(final String key, final double metric) throws MetricException {
    if (!isStarted) {
      throw new MetricException("MetricTracker is not started");
    }
    if (metrics.containsKey(key)) {
      throw new MetricException("MetricTracker already has this metric");
    }
    metrics.put(key, metric);
  }
}
