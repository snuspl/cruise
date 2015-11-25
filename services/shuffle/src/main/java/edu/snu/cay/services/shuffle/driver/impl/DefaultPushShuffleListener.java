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
package edu.snu.cay.services.shuffle.driver.impl;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Default implementation of PushShuffleListener.
 */
public class DefaultPushShuffleListener implements PushShuffleListener {

  private static final Logger LOG = Logger.getLogger(DefaultPushShuffleListener.class.getName());

  @Override
  public void onIterationCompleted(final IterationInfo info) {
    LOG.log(Level.INFO, "{0}th iteration is completed, {1} tuples were received.",
        new Object[]{info.getNumCompletedIterations(), info.getNumReceivedTuples()});
  }

  @Override
  public void onFinished() {
    LOG.log(Level.INFO, "The manager is finished.");
  }
}
