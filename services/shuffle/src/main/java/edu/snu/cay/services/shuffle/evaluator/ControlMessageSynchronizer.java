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
 * Evaluator-side interface to synchronize threads that expect to receive ShuffleControlMessages.
 *
 * There are latches for corresponding control message code, which have
 * two states, opened and closed. If some threads wait for a control message with specific
 * code, it makes a opened ControlMessageLatch and the thread waits on the latch until
 * the other thread release the latch with a expected ShuffleControlMessage.
 *
 * When a latch is closed with a ShuffleControlMessage, the waiting threads are released
 * if exists and waiting method return Optional.empty without any synchronization until
 * the latch is re-opened.
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
   * Open the latch for the code. It throws IllegalStateException if the latch is not closed.
   *
   * @param code a control message code
   */
  public void openLatch(final int code) {
    synchronized (this) {
      final Boolean isLatchClosed = isLatchClosedMap.get(code);
      if (isLatchClosed == null || !isLatchClosed) {
        throw new IllegalStateException("You cannot open the latch for the code[ " + code + " ] which is not closed");
      }
      isLatchClosedMap.put(code, false);
    }
  }

  /**
   * Close the latch for shuffleControlMessage. The later waitForControlMessage
   * calls does not wait until the latch is re-opened.
   *
   * @param shuffleControlMessage a shuffle control message
   */
  public void closeLatch(final ShuffleControlMessage shuffleControlMessage) {
    synchronized (this) {
      releaseLatch(shuffleControlMessage);
      isLatchClosedMap.put(shuffleControlMessage.getCode(), true);
    }
  }

  /**
   * Release the latch for shuffleControlMessage and notify all waiting threads on the latch.
   *
   * @param shuffleControlMessage a shuffle control message
   */
  public void releaseLatch(final ShuffleControlMessage shuffleControlMessage) {
    synchronized (this) {
      if (latchMap.containsKey(shuffleControlMessage.getCode())) {
        latchMap.get(shuffleControlMessage.getCode()).release(shuffleControlMessage);
        latchMap.remove(shuffleControlMessage.getCode());
      }
    }
  }

  /**
   * Wait for the other thread releasing the latch for the code.
   * It returns Optional.empty if the latch is closed.
   *
   * @param code a control message code
   * @return the expected ShuffleControlMessage
   */
  public Optional<ShuffleControlMessage> waitForControlMessage(final int code) {
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

    return controlMessageLatch.waitForControlMessage();
  }
}

final class ControlMessageLatch {

  private boolean released;
  private ShuffleControlMessage expectedControlMessage;
  private final int expectedCode;

  ControlMessageLatch(final int expectedCode) {
    this.expectedCode = expectedCode;
  }

  /**
   * Release the latch with the controlMessage
   *
   * @param controlMessage a shuffle control message
   */
  void release(final ShuffleControlMessage controlMessage) {
    synchronized (this) {
      if (controlMessage.getCode() != expectedCode) {
        throw new IllegalArgumentException("The expected code is " + expectedCode + " but the latch is released with "
            + controlMessage.getCode());
      }

      if (!released) {
        expectedControlMessage = controlMessage;
        released = true;
        notifyAll();
      }
    }
  }

  /**
   * Wait for the other thread releasing the latch
   *
   * @return the expected ShuffleControlMessage
   */
  Optional<ShuffleControlMessage> waitForControlMessage() {
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
