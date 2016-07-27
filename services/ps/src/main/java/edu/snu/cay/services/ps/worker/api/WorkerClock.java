package edu.snu.cay.services.ps.worker.api;

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

/**
 * A worker clock which ticks on each call of clock().
 * It also contains global minimum clock among all workers.
 */
public interface WorkerClock {

  /**
   * Set initial worker clock.
   * initial worker clock =
   * global minimum clock + ({@link edu.snu.cay.services.ps.worker.parameters.Staleness}/2)
   */
  void initialize();

  /**
   * Tick worker clock.
   */
  void clock();

  /**
   * @return current worker clock
   */
  int getWorkerClock();

  /**
   * @return global minimum clock among all workers
   */
  int getGlobalMinimumClock();
}
