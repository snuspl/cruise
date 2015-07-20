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
import java.util.concurrent.ConcurrentHashMap;

/**
 * Metric tracker for intervals, which are generated from start() to stop().
 * The beginning and end of each interval are specified via beginInterval(key)
 * and endInterval(key).
 *
 * MetricException is thrown when the beginInterval() is called more than once
 * for the same key, or endInterval() is called for the key that does not exist.
 *
 * Note the intervals that do not call endInterval() are ignored.
 */
public class MetricTrackerInterval implements MetricTracker {

  /**
   * Key to get/set the interval of sending data in the ComputeTask.
   */
  public static final String KEY_METRIC_TASK_SEND_DATA = "METRIC_TASK_SEND_DATA";

  /**
   * Key to get/set the interval of computation in the ComputeTask.
   */
   public static final String KEY_METRIC_TASK_COMPUTE = "METRIC_TASK_COMPUTE";

  /**
   * Key to get/set the interval of receiving data in the ComputeTask.
   */
  public static final String KEY_METRIC_TASK_RECEIVE_DATA = "METRIC_TASK_RECEIVE_DATA";

  /**
   * The moments when startInterval() are called.
   * This should be thread-safe because multiple threads can access it simultaneously.
   */
  private final Map<String, Double> beginTimes = new ConcurrentHashMap<>();

  /**
   * The intervals that endInterval() are called.
   * This should be thread-safe because multiple threads can access it simultaneously.
   */
  private final Map<String, Double> intervals = new ConcurrentHashMap<>();

  @Inject
  private MetricTrackerInterval() {
  }

  @Override
  public void start() {
  }

  @Override
  public Map<String, Double> stop() {
    final Map<String, Double> result = new HashMap<>();
    result.putAll(intervals);

    beginTimes.clear();
    intervals.clear();

    return result;
  }

  @Override
  public void close() throws Exception {
    // Do nothing.
  }

  /**
   * Mark the beginning of interval.
   * @param key identifier to distinguish the interval
   * @throws MetricException if beginInterval() was already called on the same key.
   */
  public void beginInterval(final String key) throws MetricException {
    if (beginTimes.containsKey(key)) {
      throw new MetricException("beginInterval() was already called on " + key);
    } else {
      final long beginTime = System.currentTimeMillis();
      beginTimes.put(key, (double) beginTime);
    }
  }

  /**
   * Mark the end of interval.
   * @param key identifier to distinguish the interval
   * @throws MetricException if endInterval() was already called on the same key,
   *         or beginInterval() was not called on the key
   */
  public void endInterval(final String key) throws MetricException {
    if (intervals.containsKey(key)) {
      throw new MetricException("endInterval() was already called on " + key);
    }

    if (beginTimes.containsKey(key)) {
      final long endTime = System.currentTimeMillis();
      final double beginTime = beginTimes.get(key);
      intervals.put(key, (double) endTime - beginTime);
    } else {
      throw new MetricException("beginInterval() was not called on " + key);
    }
  }
}
