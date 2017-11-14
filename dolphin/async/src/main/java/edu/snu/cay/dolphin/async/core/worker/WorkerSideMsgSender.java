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
package edu.snu.cay.dolphin.async.core.worker;

import edu.snu.cay.dolphin.async.*;
import edu.snu.cay.dolphin.async.network.NetworkConnection;
import edu.snu.cay.services.et.configuration.parameters.ExecutorIdentifier;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

import java.nio.ByteBuffer;

/**
 * Worker-side message sender.
 */
@EvaluatorSide
public final class WorkerSideMsgSender {

  private final String driverId;
  private final String dolphinJobId;
  private final String executorId;
  private final NetworkConnection<DolphinMsg> networkConnection;

  private final SerializableCodec<WorkerGlobalBarrier.State> codec;

  @Inject
  private WorkerSideMsgSender(final NetworkConnection<DolphinMsg> networkConnection,
                              final SerializableCodec<WorkerGlobalBarrier.State> codec,
                              @Parameter(DriverIdentifier.class) final String driverId,
                              @Parameter(DolphinParameters.DolphinJobId.class) final String dolphinJobId,
                              @Parameter(ExecutorIdentifier.class) final String executorId) {
    this.networkConnection = networkConnection;
    this.driverId = driverId;
    this.dolphinJobId = dolphinJobId;
    this.executorId = executorId;
    this.codec = codec;
  }

  /**
   * Send {@link ProgressMsg} to master-side for every epoch.
   * @param epochIdx a current processing epoch index
   */
  void sendEpochProgressMsg(final int epochIdx) throws NetworkException {
    final ProgressMsg progressMsg = ProgressMsg.newBuilder()
        .setExecutorId(executorId)
        .setType(ProgressMsgType.Epoch)
        .setProgress(epochIdx)
        .build();

    final DolphinMsg dolphinMsg = DolphinMsg.newBuilder()
        .setJobId(dolphinJobId)
        .setType(dolphinMsgType.ProgressMsg)
        .setProgressMsg(progressMsg)
        .build();

    networkConnection.send(driverId, dolphinMsg);
  }

  /**
   * Send {@link ProgressMsg} to master-side for every mini-batch.
   * @param batchIdx a current processing mini-batch index
   */
  void sendBatchProgressMsg(final int batchIdx) throws NetworkException {
    final ProgressMsg progressMsg = ProgressMsg.newBuilder()
        .setExecutorId(executorId)
        .setType(ProgressMsgType.Batch)
        .setProgress(batchIdx)
        .build();

    final DolphinMsg dolphinMsg = DolphinMsg.newBuilder()
        .setJobId(dolphinJobId)
        .setType(dolphinMsgType.ProgressMsg)
        .setProgressMsg(progressMsg)
        .build();

    networkConnection.send(driverId, dolphinMsg);
  }

  /**
   * Send {@link SyncMsg} to master-side.
   * @param state a current state
   */
  public void sendSyncMsg(final WorkerGlobalBarrier.State state) throws NetworkException {
    final byte[] serializedState = codec.encode(state);

    final SyncMsg syncMsg = SyncMsg.newBuilder()
        .setExecutorId(executorId)
        .setSerializedState(ByteBuffer.wrap(serializedState))
        .build();

    final DolphinMsg dolphinMsg = DolphinMsg.newBuilder()
        .setJobId(dolphinJobId)
        .setType(dolphinMsgType.SyncMsg)
        .setSyncMsg(syncMsg)
        .build();

    networkConnection.send(driverId, dolphinMsg);
  }

  /**
   * Send a msg to master for asking model evaluation.
   */
  void sendModelEvalAskMsg() throws NetworkException {
    final DolphinMsg dolphinMsg = DolphinMsg.newBuilder()
        .setJobId(dolphinJobId)
        .setType(dolphinMsgType.ModelEvalAskMsg)
        .build();

    networkConnection.send(driverId, dolphinMsg);
  }
}
