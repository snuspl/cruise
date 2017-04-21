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
package edu.snu.cay.services.et.examples.metric;

import edu.snu.cay.services.et.evaluator.impl.MetricCollector;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A task that records the current timestamp and send the values as a custom metric.
 * The metrics are sent to the driver in two ways: 1) MetricCollector (ET-internal component) automatically sends
 * the metrics, and 2) Task sends the metrics by calling MetricCollector.flush() manually.
 */
final class MetricTask implements Task {
  private static final Logger LOG = Logger.getLogger(MetricTask.class.getName());

  private final long metricManualFlushPeriodMs;

  private final long customMetricRecordPeriodMs;

  private final long taskDurationMs;

  private final MetricCollector<Long> metricCollector;

  @Inject
  private MetricTask(@Parameter(MetricET.MetricManualFlushPeriodMs.class) final long metricManualFlushPeriodMs,
                     @Parameter(MetricET.CustomMetricRecordPeriodMs.class) final long customMetricRecordPeriodMs,
                     @Parameter(MetricET.TaskDurationMs.class) final long taskDurationMs,
                     final MetricCollector<Long> metricCollector) {
    this.metricManualFlushPeriodMs = metricManualFlushPeriodMs;
    this.customMetricRecordPeriodMs = customMetricRecordPeriodMs;
    this.taskDurationMs = taskDurationMs;
    this.metricCollector = metricCollector;
  }

  @Override
  public byte[] call(final byte[] bytes) throws Exception {
    LOG.log(Level.INFO, "Hello, I will record timestamp every {0} ms and flush the metrics every {1} ms manually " +
        " in addition to those are sent by MetricCollector automatically",
        new Object[] {customMetricRecordPeriodMs, metricManualFlushPeriodMs});

    final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    executor.scheduleWithFixedDelay(() -> metricCollector.addCustomMetric(System.currentTimeMillis()),
        customMetricRecordPeriodMs, customMetricRecordPeriodMs, TimeUnit.MILLISECONDS);

    executor.scheduleWithFixedDelay(metricCollector::flush,
        metricManualFlushPeriodMs, metricManualFlushPeriodMs, TimeUnit.MILLISECONDS);

    LOG.log(Level.INFO, "The main thread sleeps for {0} ms", taskDurationMs);

    try {
      Thread.sleep(taskDurationMs);
    } catch (final InterruptedException e) {
      LOG.log(Level.FINEST, "Interrupted", e);
    }

    return null;
  }
}
