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
package edu.snu.cay.services.ps.worker.impl;


import org.apache.reef.annotations.audience.EvaluatorSide;

import javax.inject.Inject;

/**
 * A worker-side clock manager.
 *
 * Receive the global minimum clock from the driver and send the worker clock to the driver.
 * Tick the worker clock after each iteration.
 */
@EvaluatorSide
public final class SSPClockManager {

  private int workerClock;

  // The minimum clock of among all worker clocks.
  private int globalMinimumClock;

  @Inject
  private SSPClockManager() {

  }

  /**
   * Set initial worker clock.
   *  initial worker clock =
   *  global minimum clock + ({@link edu.snu.cay.services.ps.worker.parameters.Staleness}/2)
   */
  public void initialize() {

  }

  /**
   * Tick worker clock.
   */
  public void clock() {

  }

  public int getWorkerClock() {
    return workerClock;
  }

  public int getGlobalMinimumClock() {
    return globalMinimumClock;
  }
}
