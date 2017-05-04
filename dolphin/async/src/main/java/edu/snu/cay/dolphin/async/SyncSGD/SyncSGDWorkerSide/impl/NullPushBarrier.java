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


import edu.snu.cay.dolphin.async.SyncSGD.SyncSGDWorkerSide.api.PushBarrier;

import javax.inject.Inject;

/**
 * This implementation is for an asynchronous system.
 */
public final class NullPushBarrier implements PushBarrier {
  @Inject
  private NullPushBarrier() {
  }

  @Override
  public void requestPushPermission() {
  }

  @Override
  public void countDownPushLatch() {
  }

  @Override
  public void prepareNextMiniBatch(final int nextRoundNum) {
  }
}
