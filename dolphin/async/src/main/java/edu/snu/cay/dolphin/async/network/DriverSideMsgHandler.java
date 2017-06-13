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
package edu.snu.cay.dolphin.async.network;


import edu.snu.cay.dolphin.async.DolphinMaster;
import edu.snu.cay.dolphin.async.DolphinMsg;
import edu.snu.cay.utils.SingleMessageExtractor;
import org.apache.reef.io.network.Message;
import org.apache.reef.tang.InjectionFuture;

import javax.inject.Inject;

/**
 * A driver-side message handler that routes messages to an appropriate component corresponding to the msg type.
 */
public final class DriverSideMsgHandler implements MessageHandler {

  private final InjectionFuture<DolphinMaster> dolphinMasterFuture;

  @Inject
  private DriverSideMsgHandler(final InjectionFuture<DolphinMaster> dolphinMasterFuture) {
    this.dolphinMasterFuture = dolphinMasterFuture;
  }

  @Override
  public void onNext(final Message<DolphinMsg> msg) {
    final DolphinMsg dolphinMsg = SingleMessageExtractor.extract(msg);

    // TODO #1175: propagate the msg to the corresponding DolphinMaster based on the job id
    final String jobId = dolphinMsg.getJobId().toString();
    dolphinMasterFuture.get().getMsgHandler().onDolphinMsg(msg.getSrcId().toString(), dolphinMsg);
  }
}
