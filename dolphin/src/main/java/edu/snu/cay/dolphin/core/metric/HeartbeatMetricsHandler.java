/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package edu.snu.cay.dolphin.core.metric;

import org.apache.reef.evaluator.context.ContextMessage;
import org.apache.reef.evaluator.context.ContextMessageSource;
import org.apache.reef.evaluator.context.parameters.ContextMessageSources;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.task.HeartBeatTriggerManager;
import org.apache.reef.util.Optional;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Sends metrics via heartbeat.
 *
 * To use this class, it must be
 * configured as a {@link MetricsHandler} implementation and
 * configured as a
 * {@link org.apache.reef.evaluator.context.parameters.ContextMessageSources}
 * Use the provided static methods to do this.
 */
public final class HeartbeatMetricsHandler implements MetricsHandler, ContextMessageSource {
  private static final Logger LOG = Logger.getLogger(HeartbeatMetricsHandler.class.getName());

  /**
   * Currently tracked metrics (Id of a metric -> value).
   * Access within a synchronized block.
   */
  private final AtomicReference<Map<String, Double>> metrics = new AtomicReference<>();

  /**
   * Codec for metrics.
   */
  private final MetricCodec metricCodec;

  /**
   * Manager of the trigger of heartbeat on which tracked metrics are sent.
   */
  private final HeartBeatTriggerManager heartBeatTriggerManager;

  /**
   * @param heartBeatTriggerManager manager for sending heartbeat to the driver
   * @param metricCodec codec for metrics
   */
  @Inject
  private HeartbeatMetricsHandler(final HeartBeatTriggerManager heartBeatTriggerManager,
                                  final MetricCodec metricCodec) {
    this.heartBeatTriggerManager = heartBeatTriggerManager;
    this.metricCodec = metricCodec;
    this.metrics.set(new HashMap<String, Double>());
  }

  @Override
  public synchronized void onNext(final Map<String, Double> newMetrics) {
    this.metrics.set(newMetrics);
    heartBeatTriggerManager.triggerHeartBeat();
  }

  /**
   * Return a message (gathered metrics) to be sent to the driver.
   * @return message
   */
  @Override
  public Optional<ContextMessage> getMessage() {
    LOG.log(Level.INFO, "Context Message Sent");
    final Map<String, Double> newMetrics = metrics.getAndSet(new HashMap<String, Double>());
    if (newMetrics.isEmpty()) {
      return Optional.empty();
    } else {
      final Optional<ContextMessage> message = Optional.of(ContextMessage.from(
          MetricsCollectionService.class.getName(),
          metricCodec.encode(newMetrics)));
      return message;
    }
  }

  /**
   * Bind this class as a MetricsHandler implementation.
   * @return configuration that binds the implementation
   */
  public static Configuration getServiceConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(MetricsHandler.class, HeartbeatMetricsHandler.class)
        .build();
  }

  /**
   * Add a context message source.
   * @return configuration to which a context message source is added
   */
  public static Configuration getContextConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindSetEntry(ContextMessageSources.class, HeartbeatMetricsHandler.class)
        .build();
  }
}
