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

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.InjectionFuture;

import javax.inject.Inject;

/**
 * A master-side message handler that routes messages to an appropriate component corresponding to the msg type.
 */
@DriverSide
public final class MasterSideMsgHandler {
  private final InjectionFuture<WorkerStateManager> workerStateManagerFuture;
  private final InjectionFuture<MiniBatchManager> miniBatchManagerFuture;
  private final InjectionFuture<ProgressTracker> progressTrackerFuture;

  @Inject
  private MasterSideMsgHandler(final InjectionFuture<WorkerStateManager> workerStateManagerFuture,
                               final InjectionFuture<MiniBatchManager> miniBatchManagerFuture,
                               final InjectionFuture<ProgressTracker> progressTrackerFuture) {
    this.workerStateManagerFuture = workerStateManagerFuture;
    this.miniBatchManagerFuture = miniBatchManagerFuture;
    this.progressTrackerFuture = progressTrackerFuture;
  }

  /**
   * Handles dolphin msgs from workers.
   */
  public void onDolphinMsg(final String srcId, final DolphinMsg dolphinMsg) {
    switch (dolphinMsg.getType()) {
    case ProgressMsg:
      progressTrackerFuture.get().onProgressMsg(dolphinMsg.getProgressMsg());
      break;
    case BatchMsg:
      miniBatchManagerFuture.get().onBatchReq(srcId, dolphinMsg.getBatchMsg().getReqMsg());
      break;
    case SyncMsg:
      workerStateManagerFuture.get().onSyncMsg(srcId, dolphinMsg.getSyncMsg());
      break;
    default:
      throw new RuntimeException("Unexpected msg type" + dolphinMsg.getType());
    }
  }
}
