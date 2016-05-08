/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.cay.async.metric;

import edu.snu.cay.async.metric.avro.MetricsMessage;
import edu.snu.cay.async.metric.avro.ServerMsg;
import edu.snu.cay.async.metric.avro.SrcType;
import edu.snu.cay.async.metric.avro.WorkerMsg;
import edu.snu.cay.common.aggregation.slave.AggregationSlave;
import edu.snu.cay.common.metric.MetricsHandler;
import edu.snu.cay.common.metric.avro.Metrics;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A MetricsHandler implementation that sends a MetricsMessage via Aggregation Service.
 * The metrics are set via MetricsHandler. The other message parts must be
 * set via the setters for each worker iteration or server window. The MetricsMessage is
 * built when sending the network message. As it builds the message incrementally,
 * this class is *not* thread-safe.
 */
public final class MetricsMessageSender implements MetricsHandler {
  private static final Logger LOG = Logger.getLogger(MetricsMessageSender.class.getName());

  private final MetricsMessageCodec metricsMessageCodec;
  private MetricsMessage.Builder metricsMessageBuilder;
  private final AggregationSlave aggregationSlave;

  @Inject
  private MetricsMessageSender(final MetricsMessageCodec metricsMessageCodec,
                               final AggregationSlave aggregationSlave) {
    this.metricsMessageCodec = metricsMessageCodec;
    this.metricsMessageBuilder = MetricsMessage.newBuilder();
    this.aggregationSlave = aggregationSlave;
  }

  public MetricsMessageSender setWorkerMsg(final WorkerMsg workerMsg) {
    metricsMessageBuilder
        .setSrcType(SrcType.Worker)
        .setWorkerMsg(workerMsg);
    return this;
  }

  public MetricsMessageSender setServerMsg(final ServerMsg serverMsg) {
    metricsMessageBuilder
        .setSrcType(SrcType.Server)
        .setServerMsg(serverMsg);
    return this;
  }

  public void send() {
    LOG.entering(MetricsMessageSender.class.getSimpleName(), "send");
    aggregationSlave.send(MetricsCollectionService.AGGREGATION_CLIENT_NAME, getMessage());
    LOG.exiting(MetricsMessageSender.class.getSimpleName(), "send");
  }

  @Override
  public void onNext(final Metrics metrics) {
    metricsMessageBuilder.setMetrics(metrics);
  }

  private byte[] getMessage() {
    final MetricsMessage metricsMessage = metricsMessageBuilder.build();
    metricsMessageBuilder = MetricsMessage.newBuilder();
    LOG.log(Level.INFO, "Sending metricsMessage {0}", metricsMessage);

    return metricsMessageCodec.encode(metricsMessage);
  }
}
