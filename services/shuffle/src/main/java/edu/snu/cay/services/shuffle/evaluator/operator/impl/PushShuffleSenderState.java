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
package edu.snu.cay.services.shuffle.evaluator.operator.impl;

import edu.snu.cay.utils.StateMachine;

/**
 * States and state machine for PushShuffleSender.
 */
public final class PushShuffleSenderState {

  public enum State {
    INIT,
    SENDING,
    COMPLETED,
    FINISHED
  }

  /**
   * @return a state machine for PushShuffleSender
   */
  public static StateMachine createStateMachine() {
    return StateMachine.newBuilder()
        .addState(State.INIT, "A sender is initialized. It sends a SENDER_INITIALIZED message to the manager")
        .addState(State.SENDING, "Sending tuples to receivers")
        .addState(State.COMPLETED, "Waiting for all receivers are completed to receive tuples from senders")
        .addState(State.FINISHED, "Finished sending data")
        .setInitialState(State.INIT)
        .addTransition(State.INIT, State.SENDING,
            "When a SENDER_CAN_SEND message arrived from the manager.")
        .addTransition(State.SENDING, State.COMPLETED,
            "When a user calls complete() method. It broadcasts SENDER_COMPLETED messages to all receivers.")
        .addTransition(State.COMPLETED, State.SENDING,
            "When a SENDER_CAN_SEND message arrived from the manager.")
        .addTransition(State.COMPLETED, State.FINISHED,
            "When a SENDER_SHUTDOWN message arrived from the manager.")
        .build();
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private PushShuffleSenderState() {
  }
}
