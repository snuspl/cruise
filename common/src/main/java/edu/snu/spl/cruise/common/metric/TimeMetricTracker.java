/*
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
package edu.snu.spl.cruise.common.metric;

import javax.inject.Inject;
import java.util.Map;
import java.util.TreeMap;

/**
 * Metric tracker for wall-clock time.
 */
public final class TimeMetricTracker implements MetricTracker {

  /**
   * key for the Wall-clock time measure.
   */
  public static final String KEY_METRIC_WALL_CLOCK_TIME = "METRIC_WALL_CLOCK_TIME";

  /**
   * elapsed time when starting to track measures.
   */
  private long startTime = 0;

  /**
   * Constructor for the wall-clock time tracker.
   * This class is instantiated by TANG.
   */
  @Inject
  public TimeMetricTracker() {
  }

  @Override
  public void start() {
    startTime = System.currentTimeMillis();
  }

  @Override
  public Map<String, Double> stop() {
    final long endTime = System.currentTimeMillis();
    final Map<String, Double> result = new TreeMap<>();
    result.put(KEY_METRIC_WALL_CLOCK_TIME, (double)(endTime - startTime));
    return result;
  }

  @Override
  public void close() {
  }
}
