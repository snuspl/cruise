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

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.Map;
import java.util.TreeMap;

/**
 * Metric tracker for maximum memory usage and average memory usage.
 *
 * This class is not thread-safe.
 * Although this class uses synchronization methods,
 * these are for synchronization between the thread using MemoryMetricTracker
 * and the Daemon thread created in the constructor.
 * This class assumes that its instance is used by one thread.
 */
public final class MemoryMetricTracker implements MetricTracker {

  /**
   * time interval between two measurements of memory usage (millisecond).
   */
  @NamedParameter(doc = "Time interval between two measurements of memory usage (millisecond)",
      short_name = "memoryMeasureInterval", default_value = "100")
  public class MeasureInterval implements Name<Long> {
  }

  /**
   * key for the Max memory measure (maximum memory usage).
   */
  public static final String KEY_METRIC_MEMORY_MAX = "METRIC_MEMORY_MAX";

  /**
   * Key for the Average memory measure (average memory usage).
   */
  public static final String KEY_METRIC_MEMORY_AVERAGE = "METRIC_MEMORY_AVERAGE";

  /**
   * value returned when memory usage was never measured.
   */
  public static final double VALUE_METRIC_MEMORY_UNKNOWN = -1.0;

  /**
   * time interval between metric tracking.
   */
  private final long measureInterval;

  /**
   * maximum memory usage.
   */
  private long maxMemory = 0;

  /**
   * sum of measured memory usage.
   */
  private long sumMemory = 0;

  /**
   * number of times memory usage is measured.
   */
  private int measureTimes = 0;

  /**
   * Whether the thread measuring memory usage should stop or not.
   */
  private boolean shouldStop = true;

  /**
   * Whether the thread measuring should close or not.
   */
  private boolean shouldTerminate = false;

  /**
   * MemoryMXBean for measuring memory usage.
   */
  private final MemoryMXBean mbean = ManagementFactory.getMemoryMXBean();

  /**
   * Constructor for the memory usage tracker, which accepts the time interval between metric tracking as a parameter.
   * This class is instantiated by TANG.
   * @param measureInterval time interval between metric tracking
   */
  @Inject
  public MemoryMetricTracker(@Parameter(MeasureInterval.class) final Long measureInterval) {
    this.measureInterval = measureInterval;
    new Thread(new Daemon()).start();
  }

  @Override
  public synchronized void start() {
    shouldStop = false;
    maxMemory = 0;
    sumMemory = 0;
    measureTimes = 0;
    this.notify();
  }

  @Override
  public synchronized Map<String, Double> stop() {
    final Map<String, Double> result = new TreeMap<>();
    shouldStop = true;
    if (measureTimes == 0) {
      result.put(KEY_METRIC_MEMORY_MAX, VALUE_METRIC_MEMORY_UNKNOWN);
      result.put(KEY_METRIC_MEMORY_AVERAGE, VALUE_METRIC_MEMORY_UNKNOWN);
    } else {
      result.put(KEY_METRIC_MEMORY_MAX, (double) maxMemory);
      result.put(KEY_METRIC_MEMORY_AVERAGE, ((double) sumMemory) / measureTimes);
    }
    return result;
  }

  @Override
  public synchronized void close() {
    shouldTerminate = true;
    this.notify();
  }

  private class Daemon implements Runnable {

    @Override
    public void run() {

      while (true) {
        synchronized (MemoryMetricTracker.this) {
          if (shouldTerminate) {
            break;
          } else if (shouldStop) {
            try {

              //wait until stop or close method is called
              MemoryMetricTracker.this.wait();
            } catch (final InterruptedException e) {
              throw new RuntimeException(e);
            }
            continue;
          } else {
            final long currentMemory = mbean.getHeapMemoryUsage().getUsed() + mbean.getNonHeapMemoryUsage().getUsed();
            maxMemory = Math.max(maxMemory, currentMemory);
            sumMemory += currentMemory;
            measureTimes += 1;
          }
        }
        try {
          Thread.sleep(measureInterval);
        } catch (final InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}
