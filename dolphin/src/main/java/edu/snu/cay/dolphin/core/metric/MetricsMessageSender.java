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

import edu.snu.cay.dolphin.core.metric.avro.ComputeMsg;
import edu.snu.cay.dolphin.core.metric.avro.ControllerMsg;
import edu.snu.cay.dolphin.core.metric.avro.IterationInfo;
import edu.snu.cay.dolphin.core.metric.avro.MetricsMessage;
import edu.snu.cay.dolphin.core.metric.avro.SrcType;
import org.apache.avro.AvroRuntimeException;
import org.apache.reef.evaluator.context.ContextMessage;
import org.apache.reef.evaluator.context.ContextMessageSource;
import org.apache.reef.evaluator.context.parameters.ContextMessageSources;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.task.HeartBeatTriggerManager;
import org.apache.reef.util.Optional;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A MetricsHandler implementation that sends a MetricsMessage via heartbeat.
 * The metrics are set via MetricsHandler. The other message parts must be
 * set via the setters for each Dolphin iteration. The MetricsMessage is
 * built when sending the heartbeat. As it builds the message incrementally,
 * this class is *not* thread-safe.
 *
 * The static methods are provided for convenience of configuration.
 *
 * TODO #172: Use NetworkConnectionService to replace the heartbeat
 */
public final class MetricsMessageSender implements MetricsHandler, ContextMessageSource {
  private static final Logger LOG = Logger.getLogger(MetricsMessageSender.class.getName());

  private final HeartBeatTriggerManager heartBeatTriggerManager;
  private final MetricCodec metricCodec;
  private final MetricsMessageCodec metricsMessageCodec;
  private MetricsMessage.Builder metricsMessageBuilder;

  @Inject
  private MetricsMessageSender(final HeartBeatTriggerManager heartbeatTriggerManager,
                               final MetricCodec metricCodec,
                               final MetricsMessageCodec metricsMessageCodec) {
    this.heartBeatTriggerManager = heartbeatTriggerManager;
    this.metricCodec = metricCodec;
    this.metricsMessageCodec = metricsMessageCodec;
    this.metricsMessageBuilder = MetricsMessage.newBuilder();
  }

  public MetricsMessageSender setComputeMsg(final ComputeMsg computeMsg) {
    metricsMessageBuilder
        .setSrcType(SrcType.Compute)
        .setComputeMsg(computeMsg);
    return this;
  }

  public MetricsMessageSender setControllerMsg(final ControllerMsg controllerMsg) {
    metricsMessageBuilder
        .setSrcType(SrcType.Controller)
        .setControllerMsg(controllerMsg);
    return this;
  }

  public MetricsMessageSender setIterationInfo(final IterationInfo iterationInfo) {
    metricsMessageBuilder
        .setIterationInfo(iterationInfo);
    return this;
  }

  public void send() {
    heartBeatTriggerManager.triggerHeartBeat();
  }

  @Override
  public void onNext(final Map<String, Double> metrics) {
    metricsMessageBuilder.setMetrics(ByteBuffer.wrap(metricCodec.encode(metrics)));
  }

  /**
   * Build the MetricsMessage, encode it, and return it as a Context heartbeat.
   * @return the Context heartbeat
   */
  @Override
  public Optional<ContextMessage> getMessage() {
    try {
      final MetricsMessage metricsMessage = metricsMessageBuilder.build();
      metricsMessageBuilder = MetricsMessage.newBuilder();
      LOG.log(Level.INFO, "Sending metricsMessage {0} as ContextMessage", metricsMessage);
      return Optional.of(ContextMessage.from(
          MetricsCollectionService.class.getName(),
          metricsMessageCodec.encode(metricsMessage)));

    } catch (final AvroRuntimeException avroRuntimeException) {
      LOG.log(Level.WARNING, "metricsMessageBuilder {0} not ready", metricsMessageBuilder);
      return Optional.empty();
    }
  }

  /**
   * Bind this class as a MetricsHandler implementation.
   * @return configuration that binds the implementation
   */
  public static Configuration getServiceConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(MetricsHandler.class, MetricsMessageSender.class)
        .build();
  }

  /**
   * Add a context message source.
   * TODO #172: This configuration can be removed when NetworkConnectionService replaces the heartbeat
   * @return configuration to which a context message source is added
   */
  public static Configuration getContextConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindSetEntry(ContextMessageSources.class, MetricsMessageSender.class)
        .build();
  }
}
