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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

final class SynchronizationTestUpdater implements ParameterUpdater<Integer, String, String> {

  private static final Logger LOG = Logger.getLogger(SynchronizationTestUpdater.class.getName());

  private final Clock clock;
  private final Map<Integer, AtomicInteger> counterMap;

  @Inject
  private SynchronizationTestUpdater(final Clock clock) {
    this.clock = clock;
    this.counterMap = new HashMap<>();
  }

  @Override
  public synchronized String process(final Integer key, final String message) {

    LOG.log(Level.INFO, "{0}-th iteration, message : {1}", new Object[]{key, message});

    if (!counterMap.containsKey(key)) {
      counterMap.put(key, new AtomicInteger(0));
    }

    final AtomicInteger counter = counterMap.get(key);

    switch (message) {
    case SynchronizationTestWorker.BEFORE_BARRIER_MSG:
      if (counter.get() < 0) {
        killEvaluatorWithMessage("A BEFORE_BARRIER_MSG in " + key +
            "-th iteration has arrived after a AFTER_BARRIER_MSG for the iteration");
      } else {
        counter.incrementAndGet();
      }
      break;
    case SynchronizationTestWorker.AFTER_BARRIER_MSG:
      if (counter.get() > 0) {
        counter.set(-counter.get());
      }

      counter.incrementAndGet();
      break;
    default:
      killEvaluatorWithMessage("Illegal message : " + message);
    }

    return message;
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
