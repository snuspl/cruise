/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.cay.async.integration;

import edu.snu.cay.services.ps.server.api.ParameterUpdater;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.Clock;
import org.apache.reef.wake.time.event.Alarm;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Workers send BEFORE_BARRIER_MSG and AFTER_BARRIER_MSG for each iteration. All AFTER_BARRIER_MSG are
 * guaranteed to arrive later than any BEFORE_BARRIER_MSG in one iteration since there are global barriers
 * between them. There is a counter for every iteration that keeps track of the number of received messages.
 * The counter is incremented when BEFORE_BARRIER_MSG arrives. Receiving the first AFTER_BARRIER_MSG, the
 * sign of the value is changed, and incremented with following BEFORE_MSG messages so that the value becomes zero
 * at the end of one iteration.
 */
final class SynchronizationTestUpdater implements ParameterUpdater<Integer, String, String> {

  private static final Logger LOG = Logger.getLogger(SynchronizationTestUpdater.class.getName());

  private final Clock clock;

  // Guarded by synchronized of process method.
  private final Map<Integer, Integer> counterMap;

  @Inject
  private SynchronizationTestUpdater(final Clock clock) {
    this.clock = clock;
    this.counterMap = new HashMap<>();
  }

  @Override
  public synchronized String process(final Integer key, final String message) {
    if (message.equals(SynchronizationTestWorker.FINAL_MSG)) {
      checkCounters();
      return message;
    }

    LOG.log(Level.INFO, "{0}-th iteration, message : {1}", new Object[]{key, message});

    if (!counterMap.containsKey(key)) {
      counterMap.put(key, 0);
    }

    int counter = counterMap.get(key);

    switch (message) {
    case SynchronizationTestWorker.BEFORE_BARRIER_MSG:
      if (counter < 0) {
        killEvaluatorWithMessage("A BEFORE_BARRIER_MSG in " + key +
            "-th iteration should not arrive after a AFTER_BARRIER_MSG for the iteration");
      } else {
        counter++;
      }
      break;
    case SynchronizationTestWorker.AFTER_BARRIER_MSG:
      if (counter > 0) {
        counter *= -1;
      }

      counter++;
      break;
    default:
      killEvaluatorWithMessage("Illegal message : " + message);
    }

    counterMap.put(key, counter);

    return message;
  }

  private void checkCounters() {
    for (final int counter : counterMap.values()) {
      if (counter != 0) {
        killEvaluatorWithMessage("All counters should be zero");
      }
    }
  }

  /**
   * Throw an exception in reef runtime threads to kill this evaluator.
   * @param message a message
   */
  private void killEvaluatorWithMessage(final String message) {
    clock.scheduleAlarm(0, new EventHandler<Alarm>() {
      @Override
      public void onNext(final Alarm alarm) {
        throw new RuntimeException(message);
      }
    });
  }

  @Override
  public String update(final String oldValue, final String deltaValue) {
    return null;
  }

  @Override
  public String initValue(final Integer key) {
    return null;
  }
}
