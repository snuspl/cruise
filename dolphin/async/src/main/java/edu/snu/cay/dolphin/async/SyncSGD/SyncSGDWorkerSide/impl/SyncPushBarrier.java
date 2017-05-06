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



import edu.snu.cay.dolphin.async.ModelAccessor;
import edu.snu.cay.dolphin.async.SyncSGD.ResettableCountDownLatch;
import edu.snu.cay.dolphin.async.SyncSGD.SyncSGDWorkerSide.api.PushBarrier;

import javax.inject.Inject;

/**
 * {@link PushBarrier} that is implemented for synchronous system.
 * {@link ModelAccessor} will be blocked in this barrier until it receives {@code PermitPushMsg} from driver.
 */
public final class SyncPushBarrier implements PushBarrier {
  private final ResettableCountDownLatch pushLatch;
  private final WorkerSideSyncSGDMsgSender msgSender;

  // thisRoundNum should be tracked to distinguish between up-to-date RequestPushPermissionMsg and deprecated
  // RequestPushPermissionMsg.
  private int thisRoundNum = 0;

  @Inject
  private SyncPushBarrier(final WorkerSideSyncSGDMsgSender msgSender) {
    this.pushLatch = new ResettableCountDownLatch(1);
    this.msgSender = msgSender;
  }

  /**
   * Send {@code RequestPushPermissionMsg} to driver and wait until this worker receives {@code PermitPushMsg}.
   */
  @Override
  public void requestPushPermission() {
    try {
      if (pushLatch.getCount() != 0) {
        msgSender.sendRequestPushPermissionMsg(thisRoundNum);
        pushLatch.await();
      }
    } catch (InterruptedException e) {
      throw new RuntimeException("Unexpected exception in SyncPushBarrier's requestPushPermission", e);
    }
  }

  /**
   * Update thisRoundNum with up-to-date value, {@code nextRoundNum}, and reset {@code pushLatch} for next mini-batch.
   * @param nextRoundNum driver notify same nextRoundNum integer value to all the workers.
   */
  @Override
  public void prepareNextMiniBatch(final int nextRoundNum) {
    thisRoundNum = nextRoundNum;
    pushLatch.reset(1);
  }

  @Override
  public void countDownPushLatch() {
    pushLatch.countDown();
  }

  /**
   * Only for SyncPushBarrierTest.
   * @return pushLatch's count
   */
  public long getLatchCount() {
    return pushLatch.getCount();
  }

  /**
   * Only for SyncPushBarrierTest.
   * @return thisRoundNum
   */
  public int getThisRoundNum() {
    return thisRoundNum;
  }
}
