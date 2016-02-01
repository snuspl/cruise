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

import edu.snu.cay.dolphin.core.avro.IterationInfo;
import edu.snu.cay.dolphin.core.metric.avro.ComputeMsg;
import edu.snu.cay.dolphin.core.metric.avro.ControllerMsg;
import edu.snu.cay.dolphin.core.metric.avro.MetricsMessage;
import edu.snu.cay.dolphin.core.metric.avro.SrcType;
import edu.snu.cay.dolphin.core.metric.ns.MetricNetworkSetup;
import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A MetricsHandler implementation that sends a MetricsMessage via NetworkConnectionService.
 * The metrics are set via MetricsHandler. The other message parts must be
 * set via the setters for each Dolphin iteration. The MetricsMessage is
 * built when sending the network message. As it builds the message incrementally,
 * this class is *not* thread-safe.
 */
public final class MetricsMessageSender implements MetricsHandler {
  private static final Logger LOG = Logger.getLogger(MetricsMessageSender.class.getName());

  private final MetricNetworkSetup metricNetworkSetup;
  private final MetricCodec metricCodec;
  private MetricsMessage.Builder metricsMessageBuilder;
  private final IdentifierFactory identifierFactory;
  private final String driverId;

  @Inject
  private MetricsMessageSender(final MetricNetworkSetup metricNetworkSetup,
                               final MetricCodec metricCodec,
                               final IdentifierFactory identifierFactory,
                               @Parameter(DriverIdentifier.class) final String driverId) {
    this.metricNetworkSetup = metricNetworkSetup;
    this.metricCodec = metricCodec;
    this.metricsMessageBuilder = MetricsMessage.newBuilder();
    this.identifierFactory = identifierFactory;
    this.driverId = driverId;
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
    LOG.entering(MetricsMessageSender.class.getSimpleName(), "send");

    final Connection<MetricsMessage> conn = metricNetworkSetup.getConnectionFactory()
        .newConnection(identifierFactory.getNewInstance(driverId));
    try {
      conn.open();
      conn.write(getMessage());
    } catch (final NetworkException ex) {
      throw new RuntimeException("NetworkException", ex);
    }

    LOG.exiting(MetricsMessageSender.class.getSimpleName(), "send");
  }

  @Override
  public void onNext(final Map<String, Double> metrics) {
    metricsMessageBuilder.setMetrics(ByteBuffer.wrap(metricCodec.encode(metrics)));
  }

  private MetricsMessage getMessage() {
    final MetricsMessage metricsMessage = metricsMessageBuilder
        .setSrcId(metricNetworkSetup.getMyId().toString())
        .build();
    metricsMessageBuilder = MetricsMessage.newBuilder();
    LOG.log(Level.INFO, "Sending metricsMessage {0}", metricsMessage);
    return metricsMessage;
  }
}
