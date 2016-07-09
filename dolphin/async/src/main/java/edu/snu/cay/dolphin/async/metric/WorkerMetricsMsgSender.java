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

import edu.snu.cay.dolphin.async.metric.avro.WorkerMetricsMsg;
import edu.snu.cay.common.aggregation.slave.AggregationSlave;
import edu.snu.cay.common.metric.MetricsHandler;
import edu.snu.cay.common.metric.avro.Metrics;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A MetricsHandler implementation that sends a WorkerMetricsMsg via Aggregation Service.
 * The metrics are set via MetricsHandler. The other message parts must be
 * set via the setters for each worker iteration or server window. The WorkerMetricsMsg is
 * built when sending the network message. As it builds the message incrementally,
 * this class is *not* thread-safe.
 */
public final class WorkerMetricsMsgSender implements MetricsHandler {
  private static final Logger LOG = Logger.getLogger(WorkerMetricsMsgSender.class.getName());

  private final WorkerMetricsMsgCodec workerMetricsMsgCodec;
  private WorkerMetricsMsg.Builder metricsMessageBuilder;
  private final AggregationSlave aggregationSlave;

  @Inject
  private WorkerMetricsMsgSender(final WorkerMetricsMsgCodec workerMetricsMsgCodec,
                                 final AggregationSlave aggregationSlave) {
    this.workerMetricsMsgCodec = workerMetricsMsgCodec;
    this.metricsMessageBuilder = WorkerMetricsMsg.newBuilder();
    this.aggregationSlave = aggregationSlave;
  }

  /**
   * Sends the WorkerMetricsMsg with iteration and data information via AggregationService.
   * @param iteration The iteration number from which the Metrics are collected
   * @param numDataBlocks The number of data blocks in the local MemoryStore
   */
  public void send(final int iteration, final int numDataBlocks) {
    LOG.entering(WorkerMetricsMsgSender.class.getSimpleName(), "send");
    aggregationSlave.send(ConstantsForWorker.AGGREGATION_CLIENT_NAME, getMessage(iteration, numDataBlocks));
    LOG.exiting(WorkerMetricsMsgSender.class.getSimpleName(), "send");
  }

  @Override
  public void onNext(final Metrics metrics) {
    metricsMessageBuilder.setMetrics(metrics);
  }

  private byte[] getMessage(final int iteration, final int numDataBlocks) {
    final WorkerMetricsMsg metricsMessage = metricsMessageBuilder
        .setIteration(iteration)
        .setNumDataBlocks(numDataBlocks)
        .build();
    metricsMessageBuilder = WorkerMetricsMsg.newBuilder();
    LOG.log(Level.INFO, "Sending metricsMessage {0}", metricsMessage);

    return workerMetricsMsgCodec.encode(metricsMessage);
  }
}
