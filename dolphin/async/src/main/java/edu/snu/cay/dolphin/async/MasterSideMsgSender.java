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
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.exception.evaluator.NetworkException;

import javax.inject.Inject;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Master-side message sender.
 */
@DriverSide
final class MasterSideMsgSender {
  private static final Logger LOG = Logger.getLogger(MasterSideMsgSender.class.getName());

  private static final DolphinMsg RELEASE_MSG = DolphinMsg.newBuilder()
      .setType(dolphinMsgType.ReleaseMsg)
      .build();

  private final NetworkConnection<DolphinMsg> networkConnection;

  @Inject
  private MasterSideMsgSender(final NetworkConnection<DolphinMsg> networkConnection) {
    LOG.log(Level.INFO, "the constructor of MasterSideMsgSender");
    this.networkConnection = networkConnection;
  }

  /**
   * Send a release msg to {@code workerId}.
   * @param workerId an identifier of worker
   */
  void sendReleaseMsg(final String workerId) {
    try {
      networkConnection.send(workerId, RELEASE_MSG);
    } catch (NetworkException e) {
      LOG.log(Level.INFO, String.format("Fail to send msg to worker %s.", workerId), e);
    }
  }
}
