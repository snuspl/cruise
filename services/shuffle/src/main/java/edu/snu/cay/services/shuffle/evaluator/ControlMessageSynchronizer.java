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
package edu.snu.cay.services.shuffle.evaluator;

import edu.snu.cay.services.shuffle.network.ShuffleControlMessage;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.util.Optional;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Evaluator-side interface to synchronize threads that wait for receiving specific ShuffleControlMessages.
 *
 * First, threads should wait on a latch for a certain ShuffleControlMessage, specified by a code.
 * Second, the waiting threads are woken up by another thread that resets the latch.
 * Finally, the latch may be closed in case additional threads try to wait on the latch when waiting is unnecessary.
 * The latch can be reopened for later use.
 */
@EvaluatorSide
@ThreadSafe
public final class ControlMessageSynchronizer {

  /**
   * Map of ControlMessageLatches. There is a ControlMessageLatches for
   * a certain code in the map only when some threads are waiting the code on the latch.
   */
  private final Map<Integer, ControlMessageLatch> latchMap;

  /**
   * Indicates whether a latch for a certain code is closed or not.
   */
  private final Map<Integer, Boolean> isLatchClosedMap;

  /**
   * Maintaining the ShuffleControlMessage which was used to closed a latch.
   */
  private final Map<Integer, ShuffleControlMessage> closedControlMessageMap;

  @Inject
  private ControlMessageSynchronizer() {
    this.latchMap = new HashMap<>();
    this.isLatchClosedMap = new HashMap<>();
    this.closedControlMessageMap = new HashMap<>();
  }

  /**
   * Wait certain amount of time until another thread resets the latch
   * for the ShuffleControlMessage that is specified by the given code.
   * If timeout is zero, callers endlessly wait for resetting.
   *
   * If the latch is closed, it returns the saved control message that is used for closing the latch.
   * It returns Optional.empty if the specified time elapsed without resetting.
   *
   * @param code a control message code
   * @param timeout the maximum time to wait in milliseconds.
   * @return the expected ShuffleControlMessage
   */
  public Optional<ShuffleControlMessage> waitOnLatch(final int code, final long timeout) {
    final ControlMessageLatch controlMessageLatch;
    synchronized (this) {
      final Boolean isLatchClosed = isLatchClosedMap.get(code);
      if (isLatchClosed != null && isLatchClosed) {
        return Optional.of(closedControlMessageMap.get(code));
      }

      if (!latchMap.containsKey(code)) {
        latchMap.put(code, new ControlMessageLatch(code));
      }

      controlMessageLatch = latchMap.get(code);
    }

    return controlMessageLatch.waitOnLatch(timeout);
  }

  /**
   * Release all waiting threads on the latch for shuffleControlMessage and reset the latch.
   *
   * @param shuffleControlMessage a shuffle control message
   */
  public void resetLatch(final ShuffleControlMessage shuffleControlMessage) {
    synchronized (this) {
      if (latchMap.containsKey(shuffleControlMessage.getCode())) {
        latchMap.get(shuffleControlMessage.getCode()).release(shuffleControlMessage);
        latchMap.remove(shuffleControlMessage.getCode());
      }
    }
  }

  /**
   * Reset and close the latch for shuffleControlMessage so that
   * threads that try to wait on shuffleControlMessage return without waiting.
   *
   * @param shuffleControlMessage a shuffle control message
   */
  public void closeLatch(final ShuffleControlMessage shuffleControlMessage) {
    synchronized (this) {
      resetLatch(shuffleControlMessage);
      isLatchClosedMap.put(shuffleControlMessage.getCode(), true);
      closedControlMessageMap.put(shuffleControlMessage.getCode(), shuffleControlMessage);
    }
  }

  /**
   * Reopen the latch for the code. Throws IllegalStateException if the latch is not closed.
   *
   * @param code a control message code
   */
  public void reopenLatch(final int code) {
    synchronized (this) {
      final Boolean isLatchClosed = isLatchClosedMap.get(code);
      if (isLatchClosed == null || !isLatchClosed) {
        throw new IllegalStateException("You cannot reopen the latch for the code[ " + code + " ] which is not closed");
      }

      isLatchClosedMap.put(code, false);
      closedControlMessageMap.remove(code);
    }
  }

  /**
   * Latch for a certain control message code.
   */
  private final class ControlMessageLatch {

    private final ReentrantLock lock;
    private final Condition condition;
    private boolean released;
    private ShuffleControlMessage expectedControlMessage;
    private final int expectedCode;

    private ControlMessageLatch(final int expectedCode) {
      this.expectedCode = expectedCode;
      this.lock = new ReentrantLock();
      this.condition = lock.newCondition();
    }

    /**
     * Release the latch for controlMessage.
     *
     * @param controlMessage a shuffle control message
     */
    private void release(final ShuffleControlMessage controlMessage) {
      lock.lock();
      try {
        if (controlMessage.getCode() != expectedCode) {
          throw new IllegalArgumentException("The expected code is " + expectedCode +
              " but the latch was released with " + controlMessage.getCode());
        }

        if (!released) {
          expectedControlMessage = controlMessage;
          released = true;
          condition.signalAll();
        }
      } finally {
        lock.unlock();
      }
    }

    /**
     * Wait certain amount of time until another thread releases the latch.
     * If timeout is zero, callers endlessly wait for releasing.
     *
     * It returns Optional.empty if the specified time elapsed without releasing.
     *
     * @param timeout the maximum time to wait in milliseconds.
     * @return the expected ShuffleControlMessage
     */
    private Optional<ShuffleControlMessage> waitOnLatch(final long timeout) {
      lock.lock();
      try {
        if (!released) {
          try {
            if (timeout == 0) { // wait until it is signalled
              condition.await();
            } else { // wait until it is signalled or the specified waiting time elapses.
              final boolean timeElapsed = !condition.await(timeout, TimeUnit.MILLISECONDS);
              if (timeElapsed) {
                return Optional.empty();
              }
            }
          } catch (final InterruptedException e) {
            throw new RuntimeException(e);
          }
        }

        return Optional.of(expectedControlMessage);
      } finally {
        lock.unlock();
      }
    }
  }
}
