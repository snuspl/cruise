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
package edu.snu.cay.dolphin.async;

import edu.snu.cay.dolphin.async.network.NetworkConnection;
import edu.snu.cay.services.et.configuration.parameters.ExecutorIdentifier;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.runtime.common.driver.parameters.JobIdentifier;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

import java.nio.ByteBuffer;

/**
 * Worker-side message sender.
 */
@EvaluatorSide
final class WorkerSideMsgSender {

  private final String jobId;
  private final String executorId;
  private final NetworkConnection<DolphinMsg> networkConnection;

  private final SerializableCodec<WorkerGlobalBarrier.State> codec;

  @Inject
  private WorkerSideMsgSender(final NetworkConnection<DolphinMsg> networkConnection,
                              final SerializableCodec<WorkerGlobalBarrier.State> codec,
                              @Parameter(JobIdentifier.class) final String jobId,
                              @Parameter(ExecutorIdentifier.class) final String executorId) {
    this.networkConnection = networkConnection;
    this.jobId = jobId;
    this.executorId = executorId;
    this.codec = codec;
  }

  /**
   * Send {@link ProgressMsg} to master-side.
   * @param epochIdx a current processing epoch index
   */
  void sendProgressMsg(final int epochIdx) throws NetworkException {
    final ProgressMsg progressMsg = ProgressMsg.newBuilder()
        .setExecutorId(executorId)
        .setEpochIdx(epochIdx)
        .build();

    final DolphinMsg dolphinMsg = DolphinMsg.newBuilder()
        .setType(dolphinMsgType.ProgressMsg)
        .setProgressMsg(progressMsg)
        .build();

    networkConnection.send(jobId, dolphinMsg);
  }

  /**
   * Send {@link SyncMsg} to master-side.
   * @param state a current state
   */
  void sendSyncMsg(final WorkerGlobalBarrier.State state) throws NetworkException {
    final byte[] serializedState = codec.encode(state);

    final SyncMsg syncMsg = SyncMsg.newBuilder()
        .setExecutorId(executorId)
        .setSerializedState(ByteBuffer.wrap(serializedState))
        .build();

    final DolphinMsg dolphinMsg = DolphinMsg.newBuilder()
        .setType(dolphinMsgType.SyncMsg)
        .setSyncMsg(syncMsg)
        .build();

    networkConnection.send(jobId, dolphinMsg);
  }
}
