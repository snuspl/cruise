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
package edu.snu.cay.common.metric;

import edu.snu.cay.common.metric.avro.Metrics;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Collects Metrics from the registered MetricTrackers,
 * and sends the collected metrics to the driver.
 *
 * This class is not thread-safe.
 * Although this class uses synchronization methods,
 * these are for synchronization between the thread using MetricsCollector
 * and other threads triggering heart beats.
 * This class assumes that its instance is used by one thread.
 */
public final class MetricsCollector implements AutoCloseable {
  private static final Logger LOG = Logger.getLogger(MetricsCollector.class.getName());

  /**
   * Set of registered trackers.
   */
  private final List<MetricTracker> metricTrackerList = new LinkedList<>();

  /**
   * Handler called when metrics become available.
   */
  private final MetricsHandler metricsHandler;

  /**
   * Whether tracking metrics is started or not.
   */
  private boolean isStarted = false;

  /**
   *
   */
  @Inject
  private MetricsCollector(final MetricsHandler metricsHandler) {
    this.metricsHandler = metricsHandler;
  }

  /**
   * Register metric trackers.
   * @param trackers trackers to register
   */
  public void registerTrackers(final Collection<MetricTracker> trackers) {
    metricTrackerList.addAll(trackers);
    LOG.log(Level.INFO, "Metric trackers registered");
  }

  /**
   * Start registered metric trackers.
   */
  public void start() throws MetricException {
    if (isStarted) {
      throw new MetricException("Metric tracking cannot be started again before the previous tracking finishes");
    }
    for (final MetricTracker metricTracker : metricTrackerList) {
      metricTracker.start();
    }
    isStarted = true;
  }

  /**
   * Stop registered metric trackers.
   * Gathered measures are sent to the driver.
   */
  public void stop() throws MetricException {
    if (!isStarted) {
      throw new MetricException("Metric tracking should be started first before being stopped");
    }
    final Map<CharSequence, Double> newMetrics = new HashMap<>();
    for (final MetricTracker metricTracker : metricTrackerList) {
      newMetrics.putAll(metricTracker.stop());
    }
    metricsHandler.onNext(Metrics.newBuilder().setData(newMetrics).build());
    isStarted = false;
  }

  /**
   * Close registered metric trackers.
   */
  @Override
  public void close() throws Exception {
    for (final MetricTracker metricTracker : metricTrackerList) {
      metricTracker.close();
    }
    metricTrackerList.clear();
    LOG.log(Level.INFO, "Metric trackers closed");
  }
}
