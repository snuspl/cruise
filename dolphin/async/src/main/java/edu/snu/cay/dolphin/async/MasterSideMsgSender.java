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

import edu.snu.cay.common.centcomm.master.MasterSideCentCommMsgSender;
import edu.snu.cay.utils.AvroUtils;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.exception.evaluator.NetworkException;

import javax.inject.Inject;

import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.cay.dolphin.async.ETDolphinLauncher.CENT_COMM_CLIENT_NAME;

/**
 * Master-side message sender.
 */
@DriverSide
public final class MasterSideMsgSender {
  private static final Logger LOG = Logger.getLogger(MasterSideMsgSender.class.getName());

  private static final byte[] RELEASE_MSG = AvroUtils.toBytes(
      DolphinMsg.newBuilder().setType(dolphinMsgType.ReleaseMsg).build(), DolphinMsg.class);

  private final MasterSideCentCommMsgSender masterSideCentCommMsgSender;


  @Inject
  private MasterSideMsgSender(final MasterSideCentCommMsgSender masterSideCentCommMsgSender) {
    this.masterSideCentCommMsgSender = masterSideCentCommMsgSender;
  }

  /**
   *
   * @param workerId
   */
  public void sendReleaseMsg(final String workerId) {
    try {
      masterSideCentCommMsgSender.send(CENT_COMM_CLIENT_NAME, workerId, RELEASE_MSG);
    } catch (NetworkException e) {
      LOG.log(Level.INFO, String.format("Fail to send msg to worker %s.", workerId), e);
    }
  }
}
