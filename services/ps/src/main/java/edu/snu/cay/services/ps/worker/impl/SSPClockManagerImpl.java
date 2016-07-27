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


import edu.snu.cay.services.ps.worker.api.ClockManager;
import org.apache.reef.annotations.audience.EvaluatorSide;

import javax.inject.Inject;

/**
 * A worker-side clock manager.
 *
 * Receive the global minimum clock from the driver and send the worker clock to the driver.
 * Tick the worker clock after each iteration.
 */
@EvaluatorSide
public final class SSPClockManagerImpl implements ClockManager {

  private int workerClock;

  // The minimum clock of among all worker clocks.
  private int globalMinimumClock;

  @Inject
  private SSPClockManagerImpl() {

  }

  @Override
  public void initialize() {

  }

  @Override
  public void clock() {

  }

  @Override
  public int getWorkerClock() {
    return workerClock;
  }

  @Override
  public int getGlobalMinimumClock() {
    return globalMinimumClock;
  }
}
