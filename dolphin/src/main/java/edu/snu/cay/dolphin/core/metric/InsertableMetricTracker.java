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
 * Metric tracker that accepts metrics via put(key, value).
 * Metrics begin to be collected from start() and cleared on stop().
 * So users should make sure to use unique keys between start() and stop().
 *
 * This class is thread-safe, so multiple threads can call put() simultaneously.
 */
public class InsertableMetricTracker implements MetricTracker {

  /**
   * The metrics identified by key
   */
  private final Map<String, Double> metrics = new HashMap<>();

  /**
   * Indicates that the metrics are ready to record.
   */
  private boolean isStarted = false;

  @Inject
  private InsertableMetricTracker() {
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
  public synchronized void close() throws Exception {
    // Do nothing.
  }

  /**
   * Insert a metric with given key and value
   * @param key identifier to distinguish the metric
   * @throws MetricException if the metricTracker is not between start() and stop(),
   */
  public synchronized void put(final String key, final double metric) throws MetricException {
    if (!isStarted) {
      throw new MetricException("MetricTracker is not started");
    }
    if (metrics.containsKey(key)) {
      throw new MetricException("MetricTracker already has this metric");
    }
    metrics.put(key, metric);
  }
}
