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
package edu.snu.cay.dolphin.async.core.driver.master;

import edu.snu.cay.dolphin.async.DolphinMsg;
import edu.snu.cay.dolphin.async.DolphinParameters;
import edu.snu.cay.dolphin.async.ModelEvalAnsMsg;
import edu.snu.cay.dolphin.async.dolphinMsgType;
import edu.snu.cay.dolphin.async.network.NetworkConnection;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Master-side message sender.
 */
@DriverSide
final class MasterSideMsgSender {
  private static final Logger LOG = Logger.getLogger(MasterSideMsgSender.class.getName());

  private final String dolphinJobId;

  private final DolphinMsg releaseMsg;
  private final NetworkConnection<DolphinMsg> networkConnection;

  @Inject
  private MasterSideMsgSender(@Parameter(DolphinParameters.DolphinJobId.class) final String dolphinJobId,
                              final NetworkConnection<DolphinMsg> networkConnection) {
    LOG.log(Level.INFO, "the constructor of MasterSideMsgSender");
    this.networkConnection = networkConnection;
    this.dolphinJobId = dolphinJobId;
    this.releaseMsg = DolphinMsg.newBuilder()
        .setJobId(dolphinJobId)
        .setType(dolphinMsgType.ReleaseMsg)
        .build();
  }

  /**
   * Send a release msg to {@code workerId}.
   * @param workerId an identifier of worker
   */
  void sendReleaseMsg(final String workerId) {
    try {
      networkConnection.send(workerId, releaseMsg);
    } catch (NetworkException e) {
      LOG.log(Level.INFO, String.format("Fail to send release msg to worker %s.", workerId), e);
    }
  }

  /**
   * Send a response msg for model evaluation request from a worker.
   * @param workerId a worker id
   * @param doNext a boolean indicating whether a worker do one more evaluation or not
   */
  void sendModelEvalAnsMsg(final String workerId, final boolean doNext) {
    final DolphinMsg msg = DolphinMsg.newBuilder()
        .setJobId(dolphinJobId)
        .setType(dolphinMsgType.ModelEvalAnsMsg)
        .setModelEvalAnsMsg(ModelEvalAnsMsg.newBuilder()
            .setDoNext(doNext).build())
        .build();

    try {
      networkConnection.send(workerId, msg);
    } catch (NetworkException e) {
      LOG.log(Level.INFO, String.format("Fail to send ModelEvalAns msg to worker %s.", workerId), e);
    }
  }
}
