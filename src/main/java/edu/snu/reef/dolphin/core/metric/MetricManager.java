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
package edu.snu.reef.dolphin.core.metric;


import org.apache.reef.evaluator.context.ContextMessage;
import org.apache.reef.evaluator.context.ContextMessageSource;
import org.apache.reef.task.HeartBeatTriggerManager;
import org.apache.reef.util.Optional;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Class managing registered trackers
 */
public final class MetricManager implements ContextMessageSource, AutoCloseable {
  private final static Logger LOG = Logger.getLogger(MetricManager.class.getName());

  /**
   * Set of registered trackers
   */
  private final List<MetricTracker> metricTrackerList = new LinkedList<>();

  /**
   * Currently tracked metrics (Id of a metric -> value)
   */
  private final Map<String, Double> metrics = new HashMap<>();

  /**
   * Codec for metrics
   */
  private final MetricCodec metricCodec = new MetricCodec();

  /**
   * Manager of the trigger of hear beat on which tracked metrics are sent
   */
  private final HeartBeatTriggerManager heartBeatTriggerManager;

  /**
   * This class is instantiated by TANG
   *
   * Constructor for the metric manager, which accepts Heartbeat Trigger Manager as a parameter
   * @param heartBeatTriggerManager manager for sending heartbeat to the driver
   */
  @Inject
  public MetricManager(final HeartBeatTriggerManager heartBeatTriggerManager) {
    this.heartBeatTriggerManager = heartBeatTriggerManager;
  }

  /**
   * Register metric trackers
   * @param trackers  trackers to register
   */
  public void registerTrackers(final Collection<MetricTracker> trackers) {
    metricTrackerList.addAll(trackers);
    LOG.log(Level.INFO, "Metric trackers registered");
  }

  /**
   * Start registered metric trackers
   */
  public void start() {
    for (final MetricTracker metricTracker: metricTrackerList) {
      metricTracker.start();
    }
  }

  /**
   * Stop registered metric trackers
   * Gathered measures are sent to the driver
   */
  public void stop() {
    synchronized (this) {
      for (final MetricTracker metricTracker: metricTrackerList) {
        metrics.putAll(metricTracker.stop());
      }
      heartBeatTriggerManager.triggerHeartBeat();
      metrics.clear();
    }
  }

  /**
   * Close registered metric trackers
   */
  public void close() {
    for (final MetricTracker metricTracker: metricTrackerList) {
      metricTracker.close();
    }
    metricTrackerList.clear();
    LOG.log(Level.INFO, "Metric trackers closed");
  }

  /**
   * Return a message (gathered metrics) to be sent to the driver
   * @return  message
   */
  @Override
  public Optional<ContextMessage> getMessage() {
    LOG.log(Level.INFO, "Context Message Sent");
    synchronized (this) {
      if (metrics.isEmpty()) {
        return Optional.empty();
      } else {
        final Optional<ContextMessage> message = Optional.of(ContextMessage.from(
            MetricTrackerService.class.getName(),
            metricCodec.encode(metrics)));
        return message;
      }
    }
  }
}
