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
package edu.snu.spl.cruise.ps.core.master;

import edu.snu.spl.cruise.ps.*;
import edu.snu.spl.cruise.utils.AvroUtils;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

import java.util.logging.Level;

/**
 * Master-side message sender.
 */
final class MasterSideMsgSender {
  private final JobLogger jobLogger;

  private final String cruiseJobId;

  private final InjectionFuture<ETTaskRunner> taskRunnerFuture;
  private final byte[] serializedReleaseMsg;

  @Inject
  private MasterSideMsgSender(final JobLogger jobLogger,
                              final InjectionFuture<ETTaskRunner> taskRunnerFuture,
                              @Parameter(CruiseParameters.CruiseJobId.class) final String cruiseJobId) {
    this.jobLogger = jobLogger;
    this.taskRunnerFuture = taskRunnerFuture;
    this.cruiseJobId = cruiseJobId;

    this.serializedReleaseMsg = AvroUtils.toBytes(CruiseMsg.newBuilder()
        .setJobId(cruiseJobId)
        .setType(cruiseMsgType.ReleaseMsg)
        .build(), CruiseMsg.class);
  }

  /**
   * Send a release msg to {@code workerId}.
   * @param workerId an identifier of worker
   */
  void sendReleaseMsg(final String workerId) {
    try {
      taskRunnerFuture.get().getRunningTasklet(workerId).send(serializedReleaseMsg);
    } catch (NetworkException e) {
      jobLogger.log(Level.INFO, String.format("Fail to send release msg to worker %s.", workerId), e);
    }
  }

  /**
   * Send a response msg for model evaluation request from a worker.
   * @param workerId a worker id
   * @param doNext a boolean indicating whether a worker do one more evaluation or not
   */
  void sendModelEvalAnsMsg(final String workerId, final boolean doNext) {
    final CruiseMsg msg = CruiseMsg.newBuilder()
        .setJobId(cruiseJobId)
        .setType(cruiseMsgType.ModelEvalAnsMsg)
        .setModelEvalAnsMsg(ModelEvalAnsMsg.newBuilder()
            .setDoNext(doNext).build())
        .build();

    try {
      taskRunnerFuture.get().getRunningTasklet(workerId).send(AvroUtils.toBytes(msg, CruiseMsg.class));
    } catch (NetworkException e) {
      jobLogger.log(Level.INFO, String.format("Fail to send ModelEvalAns msg to worker %s.", workerId), e);
    }
  }
}
