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

import edu.snu.cay.dolphin.async.DolphinParameters;
import edu.snu.cay.dolphin.async.metric.avro.*;
import edu.snu.cay.services.et.avro.MetricMsg;
import edu.snu.cay.services.et.avro.MetricMsgType;
import edu.snu.cay.services.et.avro.MetricReportMsg;
import edu.snu.cay.services.et.driver.api.MetricReceiver;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.cay.dolphin.async.ETModelAccessor.MODEL_TABLE_ID;
import static edu.snu.cay.dolphin.async.ETTrainingDataProvider.TRAINING_DATA_TABLE_ID;

/**
 * Implementation of Metric receiver for Dolphin on ET.
 */
public final class ETDolphinMetricReceiver implements MetricReceiver {
  private static final Logger LOG = Logger.getLogger(ETDolphinMetricReceiver.class.getName());

  private final ETDolphinMetricMsgCodec metricMsgCodec;

  private final MetricManager metricManager;

  private final int miniBatchSize;

  @Inject
  ETDolphinMetricReceiver(final ETDolphinMetricMsgCodec metricMsgCodec,
                          final MetricManager metricManager,
                          @Parameter(DolphinParameters.MiniBatchSize.class) final int miniBatchSize) {
    this.metricMsgCodec = metricMsgCodec;
    this.metricManager = metricManager;
    this.miniBatchSize = miniBatchSize;
  }

  @Override
  public void onMetricMsg(final String srcId, final MetricMsg metricMsg) {
    if (!metricMsg.getType().equals(MetricMsgType.MetricReportMsg)) {
      throw new RuntimeException(String.format("Received a message with an invalid type: %s", metricMsg.getType()));
    }

    final MetricReportMsg metricReportMsg = metricMsg.getMetricReportMsg();
    if (metricReportMsg.getTableToNumBlocks().isEmpty()) { // Tables are not prepared yet.
      return;
    }

    if (isWorkerMetrics(metricReportMsg)) {
      processWorkerMetrics(srcId, metricReportMsg);
    } else {
      processServerMetrics(srcId, metricReportMsg);
    }
  }

  /**
   * Distinguishes the metrics from workers if the metrics consist of information of the training data table.
   */
  private boolean isWorkerMetrics(final MetricReportMsg metricReportMsg) {
    return metricReportMsg.getTableToNumBlocks().containsKey(TRAINING_DATA_TABLE_ID);
  }

  /**
   * Processes worker metrics and passes them to the {@link MetricManager}.
   * For now the worker metrics are converted to be compatible to the EM's Optimizers.
   */
  // TODO #1072: Make the entire optimization pipeline use the Dolphin-on-ET-specific metrics
  private void processWorkerMetrics(final String srcId, final MetricReportMsg metricReportMsg) {
    for (final ByteBuffer encodedBuffer : metricReportMsg.getCustomMetrics()) {
      final DolphinWorkerMetrics workerMetrics = metricMsgCodec.decode(encodedBuffer.array());
      final Map<String, Integer> tableToNumBlocks = metricReportMsg.getTableToNumBlocks();
      final String hostname = metricReportMsg.getHostname();

      switch (workerMetrics.getType()) {
      case BatchMetrics:
        final BatchMetrics batchMetrics = workerMetrics.getBatchMetrics();
        final WorkerMetrics convertedBatchMetrics =
            convertBatchMetrics(metricReportMsg, tableToNumBlocks, hostname, batchMetrics);
        metricManager.storeWorkerMetrics(srcId, convertedBatchMetrics);
        break;
      case EpochMetrics:
        final EpochMetrics epochMetrics = workerMetrics.getEpochMetrics();
        final WorkerMetrics convertedEpochMetrics = convertEpochMetrics(metricReportMsg, hostname, epochMetrics);
        metricManager.storeWorkerMetrics(srcId, convertedEpochMetrics);
        break;
      default:
        throw new RuntimeException("Unknown message type");
      }

      LOG.log(Level.INFO, "Received a worker metric from {0}: {1}", new Object[] {srcId, workerMetrics});
    }
  }

  /**
   * Convert the metrics collected in an epoch to that is compatible with Dolphin-on-PS's.
   * Note that this method will be removed when we fix the workaround of using the Dolphin-on-PS's metrics.
   */
  // TODO #1072: Make the entire optimization pipeline use the Dolphin-on-ET-specific metrics
  private WorkerMetrics convertEpochMetrics(final MetricReportMsg metricReportMsg,
                                            final String hostname,
                                            final EpochMetrics epochMetrics) {
    return WorkerMetrics.newBuilder()
              .setEpochIdx(epochMetrics.getEpochIdx())
              .setMiniBatchSize(miniBatchSize)
              .setNumMiniBatchForEpoch(epochMetrics.getNumBatchesForEpoch())
              .setProcessedDataItemCount(epochMetrics.getNumEpochDataInstances())
              .setNumDataBlocks(metricReportMsg.getTableToNumBlocks().get(TRAINING_DATA_TABLE_ID))
              .setTotalTime(epochMetrics.getEpochTimeSec())
              .setTotalCompTime(epochMetrics.getEpochCompTimeSec())
              .setTotalPullTime(epochMetrics.getEpochPullTimeSec())
              .setTotalPushTime(epochMetrics.getEpochPushTimeSec())
              .setParameterWorkerMetrics(buildParameterWorkerMetrics(metricReportMsg))
              .setHostname(hostname)
              .build();
  }

  /**
   * Convert the metrics collected in a batch to that is compatible with Dolphin-on-PS's.
   * Note that this method will be removed when we fix the workaround of using the Dolphin-on-PS's metrics.
   */
  // TODO #1072: Make the entire optimization pipeline use the Dolphin-on-ET-specific metrics
  private WorkerMetrics convertBatchMetrics(final MetricReportMsg metricReportMsg,
                                            final Map<String, Integer> tableToNumBlocks,
                                            final String hostname,
                                            final BatchMetrics batchMetrics) {
    return WorkerMetrics.newBuilder()
              .setNumDataBlocks(tableToNumBlocks.get(TRAINING_DATA_TABLE_ID))
              .setEpochIdx(batchMetrics.getEpochIdx())
              .setMiniBatchIdx(batchMetrics.getBatchIdx())
              .setMiniBatchSize(miniBatchSize)
              .setProcessedDataItemCount(batchMetrics.getNumBatchDataInstances())
              .setTotalTime(batchMetrics.getBatchTimeSec())
              .setTotalCompTime(batchMetrics.getBatchCompTimeSec())
              .setTotalPullTime(batchMetrics.getBatchPullTimeSec())
              .setTotalPushTime(batchMetrics.getBatchPushTimeSec())
              .setParameterWorkerMetrics(buildParameterWorkerMetrics(metricReportMsg))
              .setHostname(hostname)
              .build();
  }

  /**
   * Build a ParameterWorkerMetrics that Dolphin-on-PS uses.
   * Note that this method will be removed when we fix the workaround of using the Dolphin-on-PS's metrics.
   */
  // TODO #1072: Make the entire optimization pipeline use the Dolphin-on-ET-specific metrics
  private ParameterWorkerMetrics buildParameterWorkerMetrics(final MetricReportMsg metricReportMsg) {
    return ParameterWorkerMetrics.newBuilder()
        .setTotalPullCount(metricReportMsg.getCountSentGetReq().getOrDefault(MODEL_TABLE_ID, 0))
        .setTotalReceivedBytes(metricReportMsg.getBytesReceivedGetResp().getOrDefault(MODEL_TABLE_ID, 0L))
        .build();
  }

  /**
   * Processes server metrics and passes them to the {@link MetricManager}.
   * For now the server metrics only contain ET-level information, because the current cost model does not use
   * any server-specific information.
   */
  // TODO #1104: Collect metrics from Servers in Dolphin-on-ET
  private void processServerMetrics(final String srcId, final MetricReportMsg metricReportMsg) {
    final String hostname = metricReportMsg.getHostname();
    final ServerMetrics serverMetrics = ServerMetrics.newBuilder()
        .setNumModelBlocks(metricReportMsg.getTableToNumBlocks().getOrDefault(MODEL_TABLE_ID, 0))
        .setHostname(hostname)
        .build();
    metricManager.storeServerMetrics(srcId, serverMetrics);
    
    LOG.log(Level.INFO, "Received a server metric from {0}: {1}", new Object[] {srcId, serverMetrics});
  }
}
