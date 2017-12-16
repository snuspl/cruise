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

import edu.snu.spl.cruise.ps.PSMsg;
import edu.snu.spl.cruise.ps.ProgressMsg;
import edu.snu.spl.cruise.utils.CatchableExecutors;
import org.apache.reef.tang.InjectionFuture;

import javax.inject.Inject;
import java.util.concurrent.ExecutorService;

/**
 * A master-side message handler that routes messages to an appropriate component corresponding to the msg type.
 */
public final class MasterSideMsgHandler {
  private final InjectionFuture<WorkerStateManager> workerStateManagerFuture;
  private final InjectionFuture<ProgressTracker> progressTrackerFuture;
  private final InjectionFuture<BatchProgressTracker> batchProgressTrackerFuture;
  private final InjectionFuture<ModelChkpManager> modelChkpManagerFuture;

  private static final int NUM_PROGRESS_MSG_THREADS = 8;
  private static final int NUM_SYNC_MSG_THREADS = 8;
  private static final int NUM_MODEL_EV_MSG_THREADS = 8;

  private final ExecutorService progressMsgExecutor = CatchableExecutors.newFixedThreadPool(NUM_PROGRESS_MSG_THREADS);
  private final ExecutorService syncMsgExecutor = CatchableExecutors.newFixedThreadPool(NUM_SYNC_MSG_THREADS);
  private final ExecutorService modelEvalMsgExecutor = CatchableExecutors.newFixedThreadPool(NUM_MODEL_EV_MSG_THREADS);

  @Inject
  private MasterSideMsgHandler(final InjectionFuture<WorkerStateManager> workerStateManagerFuture,
                               final InjectionFuture<ProgressTracker> progressTrackerFuture,
                               final InjectionFuture<BatchProgressTracker> batchProgressTrackerFuture,
                               final InjectionFuture<ModelChkpManager> modelChkpManagerFuture) {
    this.workerStateManagerFuture = workerStateManagerFuture;
    this.progressTrackerFuture = progressTrackerFuture;
    this.batchProgressTrackerFuture = batchProgressTrackerFuture;
    this.modelChkpManagerFuture = modelChkpManagerFuture;
  }

  /**
   * Handles cruise msgs from workers.
   */
  public void onPSMsg(final PSMsg cruiseMsg) {
    switch (cruiseMsg.getType()) {
    case ProgressMsg:
      final ProgressMsg progressMsg = cruiseMsg.getProgressMsg();
      switch (progressMsg.getType()) {
      case Batch:
        progressMsgExecutor.submit(() -> batchProgressTrackerFuture.get().onProgressMsg(progressMsg));
        break;
      case Epoch:
        progressMsgExecutor.submit(() -> progressTrackerFuture.get().onProgressMsg(progressMsg));
        break;
      default:
        throw new RuntimeException("Unexpected msg type");
      }
      break;
    case SyncMsg:
      syncMsgExecutor.submit(() -> workerStateManagerFuture.get().onSyncMsg(cruiseMsg.getSyncMsg()));
      break;
    case ModelEvalAskMsg:
      modelEvalMsgExecutor.submit(() -> modelChkpManagerFuture.get().onWorkerMsg());
      break;
    default:
      throw new RuntimeException("Unexpected msg type" + cruiseMsg.getType());
    }
  }
}
