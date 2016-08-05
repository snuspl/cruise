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

import edu.snu.cay.services.ps.worker.api.WorkerClock;
import org.apache.reef.annotations.audience.EvaluatorSide;

import javax.inject.Inject;

/**
 * A null worker clock(empty implementation).
 */
@EvaluatorSide
public final class NullWorkerClock implements WorkerClock {

  @Inject
  private NullWorkerClock() {

  }

  @Override
  public void initialize() {

  }

  @Override
  public void clock() {

  }

  @Override
  public void waitIfExceedingStalenessBound() {

  }

  @Override
  public int getWorkerClock() {
    return 0;
  }

  @Override
  public int getGlobalMinimumClock() {
    return 0;
  }
}
