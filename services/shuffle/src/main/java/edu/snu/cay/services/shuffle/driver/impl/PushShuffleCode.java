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

/**
 * Codes for control messages of push-based shuffle.
 */
public final class PushShuffleCode {

  /**
   * A sender was initialized.
   *
   * The sender to the manager.
   */
  public static final int SENDER_INITIALIZED = 0;

  /**
   * Notify that a sender can send data.
   *
   * The manager to the sender.
   */
  public static final int SENDER_CAN_SEND = 1;

  /**
   * A sender was completed to push data.
   *
   * The sender to all receivers.
   */
  public static final int SENDER_COMPLETED = 2;

  /**
   * Make senders be shutdown.
   *
   * The manager to all senders.
   */
  public static final int SENDER_SHUTDOWN = 3;

  /**
   * A sender was shutdown and finished by the manager.
   *
   * The sender to the manager.
   */
  public static final int SENDER_FINISHED = 4;

  /**
   * A receiver was initialized.
   *
   * The receiver to the manager.
   */
  public static final int RECEIVER_INITIALIZED = 5;

  /**
   * A receiver was completed.
   *
   * The receiver to the manager.
   */
  public static final int RECEIVER_COMPLETED = 6;

  /**
   * Notify that a receiver can receive data.
   *
   * The manager to the receiver.
   */
  public static final int RECEIVER_CAN_RECEIVE = 7;

  /**
   * A receiver was ready to receive data.
   *
   * The receiver to the manager.
   */
  public static final int RECEIVER_READY = 8;

  /**
   * Make senders be shutdown.
   *
   * The manager to all receivers.
   */
  public static final int RECEIVER_SHUTDOWN = 9;

  /**
   * A receiver was shutdown and finished by the manager.
   *
   * The receiver to the manager.
   */
  public static final int RECEIVER_FINISHED = 10;

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private PushShuffleCode() {
  }
}
