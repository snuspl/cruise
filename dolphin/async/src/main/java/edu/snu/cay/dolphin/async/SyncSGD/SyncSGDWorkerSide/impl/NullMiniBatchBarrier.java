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


import edu.snu.cay.dolphin.async.SyncSGD.SyncSGDWorkerSide.api.MiniBatchBarrier;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * {@link MiniBatchBarrier} that always permit push without asking the driver. This implementation is for an
 * asynchronous system.
 */
public final class NullMiniBatchBarrier implements MiniBatchBarrier {
  private static final Logger LOG = Logger.getLogger(NullMiniBatchBarrier.class.getName());

  @Inject
  private NullMiniBatchBarrier() {
  }

  /**
   * This method does nothing and always returns {@code LearningState.StartNextMiniBatch} because this class is for
   * asynchronous system.
   */
  @Override
  public LearningState waitMiniBatchControlMsgFromDriver() {
    LOG.log(Level.INFO, "This is NullMiniBatchBarrier");
    return LearningState.StartNextMiniBatch;
  }

  @Override
  public void startNextMiniBatch() {
  }

  @Override
  public void terminateLearning() {
  }
}
