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
 *
 */
public final class PushShuffleCode {

  public static final int SHUFFLE_INITIALIZED = 0;

  public static final int SENDER_INITIALIZED = 1;

  public static final int RECEIVER_INITIALIZED = 2;

  public static final int SENDER_COMPLETED = 3;

  public static final int ALL_SENDERS_COMPLETED = 4;

  public static final int RECEIVER_RECEIVED = 5;

  public static final int ALL_RECEIVERS_RECEIVED = 6;

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private PushShuffleCode() {
  }
}
