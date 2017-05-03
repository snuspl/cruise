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

import edu.snu.cay.dolphin.async.SyncSGD.SyncSGDDriverSide.BatchManager;

/**
 * Before {@code ModelAccessor} sends values to the server through push operation, this barrier asks
 * {@link BatchManager} whether it would be okay to send the values.
 */
public interface PushBarrier {

  /**
   * Request permission for push to the driver.
   * This method sends {@code RequestPushPermissionMsg} to the driver and waits until it receives {@code PermitPushMsg}.
   * If this worker is slow worker, this method waits until it receives {@code StartNextMiniBatchMsg}.
   */
  void requestPushPermission();

  /**
   * Count down pushLatch.
   */
  void countDownPushLatch();

  /**
   * When this worker receives {@code startNextMiniBatchMsg} from driver, PushBarrier prepares for the next mini-batch.
   * There are two things to prepare for the next mini-batch:
   * 1) Update {@code thisRoundNum} value with {@param nextRoundNum}.
   * 2) reset {@code pushLatch}.
   * @param nextRoundNum driver notify same nextRoundNum integer value to all the workers
   */
  void prepareNextMiniBatch(int nextRoundNum);
}
