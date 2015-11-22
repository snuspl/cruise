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
 * Information about one iteration.
 */
public final class IterationInfo {

  private final int numCompletedIterations;
  private final int numReceivedTuples;
  private final long elapsedTime;

  IterationInfo(final int numCompletedIterations, final int numReceivedTuples, final long elapsedTime) {
    this.numCompletedIterations = numCompletedIterations;
    this.numReceivedTuples = numReceivedTuples;
    this.elapsedTime = elapsedTime;
  }

  /**
   * The number of completed iterations of the manager.
   *
   * @return numCompletedIterations
   */
  public int getNumCompletedIterations() {
    return numCompletedIterations;
  }

  /**
   * The number of received tuples in the iteration.
   *
   * @return numReceivedTuples
   */
  public int getNumReceivedTuples() {
    return numReceivedTuples;
  }

  /**
   * Elapsed time in milliseconds.
   *
   * @return elapsedTime
   */
  public long getElapsedTime() {
    return elapsedTime;
  }
}
