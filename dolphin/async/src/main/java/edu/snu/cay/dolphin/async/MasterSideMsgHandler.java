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

import edu.snu.cay.dolphin.async.network.MessageHandler;
import edu.snu.cay.utils.SingleMessageExtractor;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.io.network.Message;
import org.apache.reef.tang.InjectionFuture;

import javax.inject.Inject;

/**
 * A master-side message handler that routes messages to an appropriate component corresponding to the msg type.
 */
@DriverSide
public final class MasterSideMsgHandler implements MessageHandler {
  private final InjectionFuture<WorkerStateManager> workerStateManagerFuture;
  private final InjectionFuture<ProgressTracker> progressTrackerFuture;

  @Inject
  private MasterSideMsgHandler(final InjectionFuture<WorkerStateManager> workerStateManagerFuture,
                               final InjectionFuture<ProgressTracker> progressTrackerFuture) {
    this.workerStateManagerFuture = workerStateManagerFuture;
    this.progressTrackerFuture = progressTrackerFuture;
  }

  @Override
  public void onNext(final Message<DolphinMsg> msg) {
    final DolphinMsg dolphinMsg = SingleMessageExtractor.extract(msg);
    switch (dolphinMsg.getType()) {
    case ProgressMsg:
      progressTrackerFuture.get().onProgressMsg(dolphinMsg.getProgressMsg());
      break;
    case SyncMsg:
      final String networkId = dolphinMsg.getSourceId().toString();
      workerStateManagerFuture.get().onSyncMsg(networkId, dolphinMsg.getSyncMsg());
      break;
    default:
      throw new RuntimeException("Unexpected msg type" + dolphinMsg.getType());
    }
  }
}
