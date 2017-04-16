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
import edu.snu.cay.dolphin.async.ETDolphinLauncher;
import edu.snu.cay.dolphin.async.metric.avro.*;
import edu.snu.cay.services.et.avro.MetricMsg;
import edu.snu.cay.services.et.driver.api.MetricReceiver;
import edu.snu.cay.services.ps.metric.avro.ServerMetrics;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import static edu.snu.cay.dolphin.async.ETModelAccessor.MODEL_TABLE_ID;
import static edu.snu.cay.dolphin.async.ETTrainingDataProvider.TRAINING_DATA_TABLE_ID;

/**
 * Implementation of Metric receiver for Dolphin on ET.
 */
public final class ETDolphinMetricReceiver implements MetricReceiver {
  private static final Tang TANG = Tang.Factory.getTang();

  private final ETDolphinMetricMsgCodec metricMsgCodec;

  private final MetricManager metricManager;

  private final int miniBatchSize;

  @Inject
  ETDolphinMetricReceiver(final ETDolphinMetricMsgCodec metricMsgCodec,
                          final MetricManager metricManager,
                          @Parameter(ETDolphinLauncher.SerializedWorkerConf.class) final String serializedWorkerConf) {
    this.metricMsgCodec = metricMsgCodec;
    this.metricManager = metricManager;
    try {
      final Configuration workerConf = TANG.newInjector().getInstance(ConfigurationSerializer.class)
          .fromString(serializedWorkerConf);
      this.miniBatchSize = TANG.newInjector(workerConf).getNamedInstance(DolphinParameters.MiniBatchSize.class);
    } catch (IOException | InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onMetricMsg(final String srcId, final MetricMsg metricMsg) {
    if (metricMsg.getTableToNumBlocks().isEmpty()) { // Tables are not prepared yet.
      return;
    }

    if (isServerMetrics(metricMsg)) {
      processServerMetrics(srcId, metricMsg);
    } else {
      processWorkerMetrics(srcId, metricMsg);
    }
  }

  /**
   * Distinguishes the server metrics if the metrics consist of information of the model table.
   */
  private boolean isServerMetrics(final MetricMsg metricMsg) {
    return metricMsg.getTableToNumBlocks().containsKey(MODEL_TABLE_ID);
  }

  /**
   * Processes worker metrics and passes them to the {@link MetricManager}.
   * For now the worker metrics are converted to be compatible to the EM's Optimizers.
   */
  private void processWorkerMetrics(final String srcId, final MetricMsg metricMsg) {
    for (final ByteBuffer encodedBuffer : metricMsg.getCustomMetrics()) {
      final DolphinWorkerMetrics workerMetrics = metricMsgCodec.decode(encodedBuffer.array());
      final Map<String, Integer> tableToNumBlocks = metricMsg.getTableToNumBlocks();
      final String hostname = metricMsg.getHostname();

      switch (workerMetrics.getType()) {
      case BatchMetrics:
        final BatchMetrics batchMetrics = workerMetrics.getBatchMetrics();
        final WorkerMetrics convertedBatchMetrics = WorkerMetrics.newBuilder()
            .setNumDataBlocks(tableToNumBlocks.get(TRAINING_DATA_TABLE_ID))
            .setEpochIdx(batchMetrics.getEpochIdx())
            .setMiniBatchIdx(batchMetrics.getBatchIdx())
            .setMiniBatchSize(miniBatchSize)
            .setProcessedDataItemCount(batchMetrics.getNumBatchDataInstances())
            .setTotalTime(batchMetrics.getBatchTimeSec())
            .setTotalCompTime(batchMetrics.getBatchCompTimeSec())
            .setTotalPullTime(batchMetrics.getBatchPullTimeSec())
            .setTotalPushTime(batchMetrics.getBatchPushTimeSec())
            .setParameterWorkerMetrics(
                ParameterWorkerMetrics.newBuilder()
                    .setTotalPullCount(metricMsg.getCountSentGetReq().getOrDefault(MODEL_TABLE_ID, 0))
                    .setTotalReceivedBytes(metricMsg.getBytesReceivedGetResp().getOrDefault(MODEL_TABLE_ID, 0L)
                    )
                    .build()
            )
            .setHostname(hostname)
            .build();
        metricManager.storeWorkerMetrics(srcId, convertedBatchMetrics);
        break;
      case EpochMetrics:
        final EpochMetrics epochMetrics = workerMetrics.getEpochMetrics();
        final WorkerMetrics convertedEpochMetrics = WorkerMetrics.newBuilder()
            .setEpochIdx(epochMetrics.getEpochIdx())
            .setMiniBatchSize(miniBatchSize)
            .setNumMiniBatchForEpoch(epochMetrics.getNumBatchesForEpoch())
            .setProcessedDataItemCount(epochMetrics.getNumEpochDataInstances())
            .setNumDataBlocks(metricMsg.getTableToNumBlocks().get(TRAINING_DATA_TABLE_ID))
            .setTotalTime(epochMetrics.getEpochTimeSec())
            .setTotalCompTime(epochMetrics.getEpochCompTimeSec())
            .setTotalPullTime(epochMetrics.getEpochPullTimeSec())
            .setTotalPushTime(epochMetrics.getEpochPushTimeSec())
            .setParameterWorkerMetrics(
                ParameterWorkerMetrics.newBuilder()
                    .setTotalPullCount(metricMsg.getCountSentGetReq().getOrDefault(MODEL_TABLE_ID, 0))
                    .setTotalReceivedBytes(metricMsg.getBytesReceivedGetResp().getOrDefault(MODEL_TABLE_ID, 0L)
                    )
                    .build()
            )
            .setHostname(hostname)
            .build();
        metricManager.storeWorkerMetrics(srcId, convertedEpochMetrics);
        break;
      default:
        throw new RuntimeException("Unknown message type");
      }
    }
  }

  /**
   * Processes server metrics and passes them to the {@link MetricManager}.
   * For now the server metrics only contain ET-level information, because the current cost model does not use
   * any server-specific information.
   */
  // TODO #1104: Collect metrics from Servers in Dolphin-on-ET
  private void processServerMetrics(final String srcId, final MetricMsg metricMsg) {
    final String hostname = metricMsg.getHostname();
    final ServerMetrics serverMetrics = ServerMetrics.newBuilder()
        .setNumModelBlocks(metricMsg.getTableToNumBlocks().getOrDefault(MODEL_TABLE_ID, 0))
        .setHostname(hostname)
        .build();
    metricManager.storeServerMetrics(srcId, serverMetrics);
  }
}
