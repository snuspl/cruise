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
package edu.snu.cay.services.ps.metric;

import edu.snu.cay.common.aggregation.slave.AggregationSlave;
import edu.snu.cay.common.metric.MetricsHandler;
import edu.snu.cay.common.metric.avro.Metrics;
import edu.snu.cay.services.ps.metric.avro.ServerMetricsMsg;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A MetricsHandler implementation that sends a ServerMetricsMsg via Aggregation Service.
 * The metrics are set via MetricsHandler. The other message parts must be
 * set via the setters for each worker iteration or server window. The ServerMetricsMsg is
 * built when sending the network message. As it builds the message incrementally,
 * this class is *not* thread-safe.
 */
public final class ServerMetricsMsgSender implements MetricsHandler {
  private static final Logger LOG = Logger.getLogger(ServerMetricsMsgSender.class.getName());

  private final ServerMetricsMsgCodec metricsMessageCodec;
  private ServerMetricsMsg.Builder metricsMessageBuilder;
  private final AggregationSlave aggregationSlave;

  @Inject
  private ServerMetricsMsgSender(final ServerMetricsMsgCodec metricsMessageCodec,
                                 final AggregationSlave aggregationSlave) {
    this.metricsMessageCodec = metricsMessageCodec;
    this.metricsMessageBuilder = ServerMetricsMsg.newBuilder();
    this.aggregationSlave = aggregationSlave;
  }

  /**
   * Sends the ServerMetricsMsg with iteration and data information via AggregationService.
   * @param window The window number from which the Metrics are collected
   * @param numPartitionBlocks The number of partition blocks in the local MemoryStore
   */
  public void send(final int window, final int numPartitionBlocks) {
    LOG.entering(ServerMetricsMsgSender.class.getSimpleName(), "send");
    aggregationSlave.send(ConstantsForServer.AGGREGATION_CLIENT_NAME, getMessage(window, numPartitionBlocks));
    LOG.exiting(ServerMetricsMsgSender.class.getSimpleName(), "send");
  }

  @Override
  public void onNext(final Metrics metrics) {
    metricsMessageBuilder.setMetrics(metrics);
  }

  private byte[] getMessage(final int window, final int numPartitionBlocks) {
    final ServerMetricsMsg metricsMessage = metricsMessageBuilder
        .setWindow(window)
        .setNumPartitionBlocks(numPartitionBlocks)
        .build();
    metricsMessageBuilder = ServerMetricsMsg.newBuilder();
    LOG.log(Level.INFO, "Sending metricsMessage {0}", metricsMessage);

    return metricsMessageCodec.encode(metricsMessage);
  }
}
