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

  private Map<Integer, ControlMessageLatch> latchMap;
  private Map<Integer, Boolean> isLatchClosedMap;

  @Inject
  private ControlMessageSynchronizer() {
    this.latchMap = new HashMap<>();
    this.isLatchClosedMap = new HashMap<>();
  }

  /**
   * Wait for another thread to reset the latch for the ShuffleControlMessage that is specified by the given code.
   * If the latch is closed, the caller returns Optional.empty without waiting.
   *
   * @param code a control message code
   * @return the expected ShuffleControlMessage
   */
  public Optional<ShuffleControlMessage> waitOnLatch(final int code) {
    final ControlMessageLatch controlMessageLatch;
    synchronized (this) {
      final Boolean isLatchClosed = isLatchClosedMap.get(code);
      if (isLatchClosed != null && isLatchClosed) {
        return Optional.empty();
      }

      if (!latchMap.containsKey(code)) {
        latchMap.put(code, new ControlMessageLatch(code));
      }

      controlMessageLatch = latchMap.get(code);
    }

    return controlMessageLatch.waitOnLatch();
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
    }
  }

  /**
   * Latch for a certain control message code.
   */
  private final class ControlMessageLatch {

    private boolean released;
    private ShuffleControlMessage expectedControlMessage;
    private final int expectedCode;

    private ControlMessageLatch(final int expectedCode) {
      this.expectedCode = expectedCode;
    }

    /**
     * Release the latch for controlMessage.
     *
     * @param controlMessage a shuffle control message
     */
    private void release(final ShuffleControlMessage controlMessage) {
      synchronized (this) {
        if (controlMessage.getCode() != expectedCode) {
          throw new IllegalArgumentException("The expected code is " + expectedCode +
              " but the latch was released with " + controlMessage.getCode());
        }

        if (!released) {
          expectedControlMessage = controlMessage;
          released = true;
          notifyAll();
        }
      }
    }

    /**
     * Wait for another thread to release the latch.
     *
     * @return the expected ShuffleControlMessage
     */
    private Optional<ShuffleControlMessage> waitOnLatch() {
      synchronized (this) {
        try {
          while (!released) {
            wait();
          }
        } catch (final InterruptedException e) {
          throw new RuntimeException(e);
        }

        return Optional.ofNullable(expectedControlMessage);
      }
    }
  }
}
