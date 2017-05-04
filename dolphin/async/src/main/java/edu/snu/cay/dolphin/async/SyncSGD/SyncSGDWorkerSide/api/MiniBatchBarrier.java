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
package edu.snu.cay.dolphin.async.SyncSGD.SyncSGDWorkerSide.api;


import edu.snu.cay.dolphin.async.SyncSGD.SyncSGDWorkerSide.impl.WorkerSideSyncMsgHandler;
import edu.snu.cay.dolphin.async.SyncSGD.SyncSGDWorkerSide.impl.LearningState;

/**
 * Before AsyncWorkerTask starts next mini-batch, AsyncWorkerTask asks to {@code MiniBatchBarrier} whether to start next
 * mini-batch.
 */
public interface MiniBatchBarrier {

  /**
   * WorkerTask will wait in this method until this worker receives MiniBatchControlMsg from driver.
   * There are two kinds of MiniBatchControlMsg : TerminateLearningMsg, StartNextMiniBatchMsg.
   * @param epochIdx driver decides whether to progress learning or terminate learning by using this value.
   * @return If this worker receives TerminateLearningMsg from driver, this method returns
   *         {@code LearningState.TerminateLearning}.
   *         If this worker receives StartNextMiniBatchMsg from driver, this method returns
   *         {@code LearningState.ProgressLearning}.
   */
  LearningState waitMiniBatchControlMsgFromDriver(int epochIdx);

  /**
   * {@link WorkerSideSyncMsgHandler} will call this method when this worker receives
   * {@code StartNextMiniBatchMsg} from driver.
   */
  void startNextMiniBatch();

  /**
   * {@link WorkerSideSyncMsgHandler} will call this method when this worker receives
   * {@code TerminateLearningMsg} from driver.
   */
  void terminateLearning();
}
