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

package edu.snu.cay.services.shuffle.example.push;

import edu.snu.cay.services.shuffle.driver.impl.IterationInfo;
import edu.snu.cay.services.shuffle.driver.impl.PushShuffleListener;
import edu.snu.cay.services.shuffle.driver.impl.StaticPushShuffleManager;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * This shutdown the manager when the number of completed iterations reaches a certain number
 * only if the shutdown parameter was set to true.
 */
final class SimplePushShuffleListener implements PushShuffleListener {

  private static final Logger LOG = Logger.getLogger(SimplePushShuffleListener.class.getName());

  private final StaticPushShuffleManager shuffleManager;
  private final boolean shutdown;
  private final int numShutdownIteration;
  private final AtomicInteger numTotalReceivedTuples;

  SimplePushShuffleListener(final StaticPushShuffleManager shuffleManager,
      final boolean shutdown, final int numShutdownIteration) {
    this.shuffleManager = shuffleManager;
    this.shutdown = shutdown;
    this.numShutdownIteration = numShutdownIteration;
    this.numTotalReceivedTuples = new AtomicInteger();
  }

  @Override
  public void onIterationCompleted(final IterationInfo info) {
    final int numCompletedIterations = info.getNumCompletedIterations();
    final int numReceivedTuples = info.getNumReceivedTuples();
    final double elapsedTimeInSec = info.getElapsedTime() / 1000.0;
    numTotalReceivedTuples.addAndGet(info.getNumReceivedTuples());
    final double tuplesPerSec = numReceivedTuples / elapsedTimeInSec;
    LOG.log(Level.INFO, "{0} tuples were received in {1}-th iteration during {2} sec ({3} tuples / sec). Total : {4}",
        new Object[]{numReceivedTuples, numCompletedIterations, elapsedTimeInSec,
            tuplesPerSec, numTotalReceivedTuples.get()});
    if (shutdown && numCompletedIterations == numShutdownIteration) {
      LOG.log(Level.INFO, "Shut down the manager after the {0}-th iteration was completed.", numCompletedIterations);
      shuffleManager.shutdown();
    }
  }

  @Override
  public void onFinished() {
    LOG.log(Level.INFO, "The manager is finished.");
  }
}
