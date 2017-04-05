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
package edu.snu.cay.dolphin.async.metric;

import edu.snu.cay.common.metric.MetricsMsgSender;
import edu.snu.cay.dolphin.async.metric.avro.WorkerMetrics;
import edu.snu.cay.common.centcomm.slave.AggregationSlave;
import edu.snu.cay.common.metric.MetricsHandler;
import edu.snu.cay.common.metric.avro.Metrics;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import java.util.logging.Logger;

/**
 * A MetricsHandler implementation that sends a WorkerMetricsMsg via Aggregation Service.
 * The metrics are set via MetricsHandler. The other message parts must be
 * set via the setters for each worker's epochs or mini-batches.
 * The built MetricsMessage is passed through {@code send()} when sending the network message.
 */
@NotThreadSafe
public final class WorkerMetricsMsgSender implements MetricsHandler, MetricsMsgSender<WorkerMetrics> {
  private static final Logger LOG = Logger.getLogger(WorkerMetricsMsgSender.class.getName());

  private final WorkerMetricsMsgCodec workerMetricsMsgCodec;
  private final AggregationSlave aggregationSlave;
  private Metrics metrics;

  @Inject
  private WorkerMetricsMsgSender(final WorkerMetricsMsgCodec workerMetricsMsgCodec,
                                 final AggregationSlave aggregationSlave) {
    this.workerMetricsMsgCodec = workerMetricsMsgCodec;
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
  public void send(final WorkerMetrics message) {
    LOG.entering(WorkerMetricsMsgSender.class.getSimpleName(), "send");
    aggregationSlave.send(WorkerConstants.AGGREGATION_CLIENT_NAME, workerMetricsMsgCodec.encode(message));
    LOG.exiting(WorkerMetricsMsgSender.class.getSimpleName(), "send");
  }
}
