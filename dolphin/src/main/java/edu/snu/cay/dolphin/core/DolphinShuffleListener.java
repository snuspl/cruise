/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.cay.dolphin.core;

import edu.snu.cay.services.shuffle.driver.impl.IterationInfo;
import edu.snu.cay.services.shuffle.driver.impl.PushShuffleListener;

import java.util.logging.Level;
import java.util.logging.Logger;

final class DolphinShuffleListener  implements PushShuffleListener {
  private static final Logger LOG = Logger.getLogger(DolphinShuffleListener.class.getName());

  private final int stageSequence;

  DolphinShuffleListener(final int stageSequence) {
    this.stageSequence = stageSequence;
  }

  @Override
  public void onIterationCompleted(final IterationInfo info) {
    LOG.log(Level.INFO, "[Shuffle] {0}-th stage : {1}-th iteration is completed",
        new Object[]{stageSequence, info.getNumCompletedIterations()});
  }

  @Override
  public void onFinished() {
    LOG.log(Level.INFO, "[Shuffle] {0}-th stage is finished", stageSequence);
  }
}
