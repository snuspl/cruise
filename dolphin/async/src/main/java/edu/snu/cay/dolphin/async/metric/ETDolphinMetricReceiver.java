/*
 * Copyright (C) 2017 Seoul National University
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

import edu.snu.cay.dolphin.async.metric.avro.*;
import edu.snu.cay.services.et.avro.MetricMsg;
import edu.snu.cay.services.et.driver.api.MetricReceiver;
import edu.snu.cay.services.ps.metric.avro.ServerMetrics;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Implementation of Metric receiver for Dolphin on ET.
 */
public final class ETDolphinMetricReceiver implements MetricReceiver {
  private static final Logger LOG = Logger.getLogger(ETDolphinMetricReceiver.class.getName());

  private final ETDolphinMetricMsgCodec metricMsgCodec;

  private final MetricManager metricManager;

  @Inject
  ETDolphinMetricReceiver(final ETDolphinMetricMsgCodec metricMsgCodec,
                          final MetricManager metricManager) {
    this.metricMsgCodec = metricMsgCodec;
    this.metricManager = metricManager;
  }

  @Override
  public void onMetricMsg(final String srcId, final MetricMsg metricMsg) {
    if (metricMsg.getTableToNumBlocks().isEmpty()) { // Tables are not prepared yet.
      return;
    }

    if (metricMsg.getCustomMetrics() != null) { // worker
      processWorkerMetrics(srcId, metricMsg);
    } else {
      final String hostname = metricMsg.getHostname();
      final ServerMetrics serverMetrics = ServerMetrics.newBuilder()
          .setNumModelBlocks(metricMsg.getTableToNumBlocks().getOrDefault("model_table", 0))
          .setHostname(hostname)
          .build();
      metricManager.storeServerMetrics(srcId, serverMetrics);

    }
  }

  private void processWorkerMetrics(final String srcId, final MetricMsg metricMsg) {
    for (final ByteBuffer encodedBuffer : metricMsg.getCustomMetrics()) {
      final DolphinWorkerMetrics workerMetrics = metricMsgCodec.decode(encodedBuffer.array());
      final Map<String, Integer> tableToNumBlocks = metricMsg.getTableToNumBlocks();
      final String hostname = metricMsg.getHostname();

      switch (workerMetrics.getType()) {
      case BatchMetrics:
        final BatchMetrics batchMetrics = workerMetrics.getBatchMetrics();
        final WorkerMetrics workerMetrics2 = WorkerMetrics.newBuilder()
            .setNumDataBlocks(tableToNumBlocks.get("training_data_table"))
            .setEpochIdx(batchMetrics.getEpochIdx())
            .setMiniBatchIdx(batchMetrics.getBatchIdx())
            .setMiniBatchSize(batchMetrics.getBatchSize())
            .setProcessedDataItemCount(batchMetrics.getNumBatchDataInstances())
            .setTotalTime(batchMetrics.getBatchTimeSec())
            .setTotalCompTime(batchMetrics.getBatchCompTimeSec())
            .setTotalPullTime(batchMetrics.getBatchPullTimeSec())
            .setTotalPushTime(batchMetrics.getBatchPushTimeSec())
            .setParameterWorkerMetrics(
                ParameterWorkerMetrics.newBuilder()
                    .setTotalPullCount(
                        metricMsg.getGetNetworkStat()
                            .getNumSentReqCount().getOrDefault("model_table", 0))
                    .setTotalReceivedBytes(
                        metricMsg.getGetNetworkStat()
                            .getNumReceivedBytes().getOrDefault("model_table", 0L))
                    .build())
            .setHostname(hostname)
            .build();
        metricManager.storeWorkerMetrics(srcId, workerMetrics2);
        break;
      case EpochMetrics:
        final EpochMetrics epochMetrics = workerMetrics.getEpochMetrics();
        final WorkerMetrics workerMetrics1 = WorkerMetrics.newBuilder()
            .setEpochIdx(epochMetrics.getEpochIdx())
            .setMiniBatchSize(epochMetrics.getBatchSize())
            .setNumMiniBatchForEpoch(epochMetrics.getNumBatchesForEpoch())
            .setProcessedDataItemCount(epochMetrics.getNumEpochDataInstances())
            .setNumDataBlocks(metricMsg.getTableToNumBlocks().get("training_data_table                                                                  "))
            .setTotalTime(epochMetrics.getEpochTimeSec())
            .setTotalCompTime(epochMetrics.getEpochCompTimeSec())
            .setTotalPullTime(epochMetrics.getEpochPullTimeSec())
            .setTotalPushTime(epochMetrics.getEpochPushTimeSec())
            .setParameterWorkerMetrics(
                ParameterWorkerMetrics.newBuilder()
                    .setTotalPullCount(
                        metricMsg.getGetNetworkStat()
                            .getNumSentReqCount().getOrDefault("model_table", 0))
                    .setTotalReceivedBytes(
                        metricMsg.getGetNetworkStat()
                            .getNumReceivedBytes().getOrDefault("model_table", 0L))
                    .build())
            .setHostname(hostname)
            .build();
        metricManager.storeWorkerMetrics(srcId, workerMetrics1);
        break;
      default:
        throw new RuntimeException("Unknown message type");
      }
    }
  }
}
