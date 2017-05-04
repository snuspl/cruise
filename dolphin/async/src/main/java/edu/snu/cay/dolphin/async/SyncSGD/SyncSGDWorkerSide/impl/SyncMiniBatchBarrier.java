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
package edu.snu.cay.dolphin.async.SyncSGD.SyncSGDWorkerSide.impl;


import edu.snu.cay.dolphin.async.SyncSGD.ResettableCountDownLatch;
import edu.snu.cay.dolphin.async.SyncSGD.SyncSGDWorkerSide.api.MiniBatchBarrier;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * {@link MiniBatchBarrier} that is implemented for synchronous system.
 * This worker will be blocked in this barrier until it receives {@code StartNextMiniBatchMsg} or
 * {@code TerminateLearningMsg} from driver.
 */
public final class SyncMiniBatchBarrier implements MiniBatchBarrier {
  private static final Logger LOG = Logger.getLogger(SyncMiniBatchBarrier.class.getName());
  private final ResettableCountDownLatch miniBatchLatch;
  private volatile LearningState learningState = LearningState.ProgressLearning;
  private final WorkerSideSyncSGDMsgSender msgSender;

  @Inject
  private SyncMiniBatchBarrier(final WorkerSideSyncSGDMsgSender workerSideSyncSGDMsgSender) {
    this.miniBatchLatch = new ResettableCountDownLatch(1);
    this.msgSender = workerSideSyncSGDMsgSender;
  }

  /**
   * When this worker receives MiniBatchControlMsg from driver, {@link WorkerSideSyncSGDMsgHandler} will count down
   * {@code miniBatchLatch}.
   * @param epochIdx driver decides whether to progress learning or terminate learning by using this value.
   * @return learning state decided by driver.
   */
  @Override
  public LearningState waitMiniBatchControlMsgFromDriver(final int epochIdx) {
    try {
      LOG.log(Level.INFO, "Mini-batch is finished. Waiting for MiniBatchControlMsg.");
      msgSender.sendMiniBatchFinishedMsg(epochIdx);
      miniBatchLatch.await();
      miniBatchLatch.reset(1);
    } catch (InterruptedException e) {
      throw new RuntimeException("Unexpected exception in SyncMiniBatchBarrier" + e);
    }
    return learningState;
  }

  @Override
  public void startNextMiniBatch() {
    miniBatchLatch.countDown();
  }

  @Override
  public void terminateLearning() {
    learningState = LearningState.TerminateLearning;
    miniBatchLatch.countDown();
  }
}
