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
 * States and state machine for PushShuffleReceiver.
 */
public final class PushShuffleReceiverState {

  public static final String RECEIVING = "RECEIVING";
  public static final String COMPLETED = "COMPLETED";
  public static final String FINISHED = "FINISHED";

  /**
   * @return a state machine for PushShuffleReceiver
   */
  public static StateMachine createStateMachine() {
    return StateMachine.newBuilder()
        .addState(RECEIVING, "Receiving data from senders."
            + " It sends a RECEIVER_INITIALIZED message to the manager when it is initialized.")
        .addState(COMPLETED, "Completed to receive data from all senders in one iteration")
        .addState(FINISHED, "Finished receiving data")
        .setInitialState(RECEIVING)
        .addTransition(RECEIVING, COMPLETED,
            "When SENDER_COMPLETED messages arrived from all senders."
                + " It sends a RECEIVER_COMPLETED message to the manager.")
        .addTransition(COMPLETED, RECEIVING,
            "When a RECEIVER_CAN_RECEIVE message arrived from the manager."
                + " It sends a RECEIVER_READIED message to the manager.")
        .addTransition(COMPLETED, FINISHED,
            "When a RECEIVER_SHUTDOWN message arrived from the manager.")
        .build();
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private PushShuffleReceiverState() {
  }
}
