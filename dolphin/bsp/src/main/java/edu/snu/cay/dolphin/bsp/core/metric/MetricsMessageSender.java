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
package edu.snu.cay.dolphin.bsp.core.metric;

import edu.snu.cay.common.aggregation.slave.AggregationSlave;
import edu.snu.cay.common.metric.MetricsHandler;
import edu.snu.cay.common.metric.MetricsMsgSender;
import edu.snu.cay.common.metric.avro.Metrics;
import edu.snu.cay.dolphin.bsp.core.metric.avro.MetricsMessage;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A MetricsHandler implementation that sends a MetricsMessage via Aggregation Service.
 * The metrics are set via MetricsHandler. The other message parts must be
 * set via the setters for each Dolphin iteration. The MetricsMessage is
 * built when sending the network message. As it builds the message incrementally,
 */
@NotThreadSafe
public final class MetricsMessageSender implements MetricsHandler, MetricsMsgSender<MetricsMessage> {
  private static final Logger LOG = Logger.getLogger(MetricsMessageSender.class.getName());

  private final MetricsMessageCodec metricsMessageCodec;
  private final AggregationSlave aggregationSlave;
  private Metrics metrics;

  @Inject
  private MetricsMessageSender(final MetricsMessageCodec metricsMessageCodec,
                               final AggregationSlave aggregationSlave) {
    this.metricsMessageCodec = metricsMessageCodec;
    this.aggregationSlave = aggregationSlave;
  }

  @Override
  public void onNext(final Metrics metricsParam) {
    this.metrics = metricsParam;
  }

  @Override
  public Metrics getMetrics() {
    return metrics;
  }

  @Override
  public void send(final MetricsMessage message) {
    LOG.log(Level.INFO, "Sending metricsMessage {0}", message);
    LOG.entering(MetricsMessageSender.class.getSimpleName(), "send");
    aggregationSlave.send(MetricsMessageSender.class.getName(), metricsMessageCodec.encode(message));
    LOG.exiting(MetricsMessageSender.class.getSimpleName(), "send");
  }
}
