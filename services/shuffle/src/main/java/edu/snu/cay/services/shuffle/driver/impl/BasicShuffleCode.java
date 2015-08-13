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

// TODO (#82) : This class will be removed when BasicShuffle and BasicShuffleManager are removed.
/**
 * Control message codes for basic shuffle classes.
 * The prefix of codes represents where the control message was sent from.
 */
public final class BasicShuffleCode {

  /**
   * An end point is setup.
   */
  public static final int SHUFFLE_SETUP = 0;

  /**
   * All end points in the shuffle are setup.
   */
  public static final int MANAGER_SETUP = 1;

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private BasicShuffleCode() {
  }
}
