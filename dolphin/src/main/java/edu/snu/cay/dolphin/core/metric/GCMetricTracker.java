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
package edu.snu.cay.dolphin.core.metric;

import javax.inject.Inject;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.TreeMap;

/**
 * Metric tracker for the number of garbage collections and the elapsed time for garbage collections
 */
public final class GCMetricTracker implements MetricTracker {

  /**
   * key for the GC count measure (the number of garbage collection)
   */
  public static final String KEY_METRIC_GC_COUNT = "METRIC_GC_COUNT";

  /**
   * key for the GC time measure (elapsed time for garbage collection)
   */
  public static final String KEY_METRIC_GC_TIME = "METRIC_GC_TIME";

  /**
   * total number of garbage collections that have occurred when starting to track measures
   */
  private int startGCCount = 0;

  /**
   * approximate accumulated garbage collection elapsed time in milliseconds when starting to track measures
   */
  private long startGCTime = 0;

  /**
   * This class is instantiated by TANG
   *
   * Constructor for the Garbage Collector tracker
   */
  @Inject
  public GCMetricTracker() {
  }

  @Override
  public void start() {
    startGCCount = 0;
    startGCTime = 0;
    for(final GarbageCollectorMXBean garbageCollectorMXBean : ManagementFactory.getGarbageCollectorMXBeans()) {
      final long localCount = garbageCollectorMXBean.getCollectionCount();
      final long localTime = garbageCollectorMXBean.getCollectionTime();
      if (localCount > 0) {
        startGCCount += localCount;
      }
      if (localTime > 0) {
        startGCTime = localTime;
      }
    }
  }

  @Override
  public Map<String, Double> stop() {
    long endGCCount = 0;
    long endGCTime = 0;
    for (final GarbageCollectorMXBean garbageCollectorMXBean : ManagementFactory.getGarbageCollectorMXBeans()) {
      final long localCount = garbageCollectorMXBean.getCollectionCount();
      final long localTime = garbageCollectorMXBean.getCollectionTime();
      if (localCount > 0) {
        endGCCount += localCount;
      }
      if (localTime > 0) {
        endGCTime = localTime;
      }
    }

    final Map<String, Double> result = new TreeMap<>();
    result.put(KEY_METRIC_GC_COUNT, (double) (endGCCount - startGCCount));
    result.put(KEY_METRIC_GC_TIME, (double) (endGCTime - startGCTime));
    return result;
  }

  @Override
  public void close() {
  }
}
