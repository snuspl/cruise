/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.spl.cruise.ps.optimizer.api;

import edu.snu.spl.cruise.ps.optimizer.impl.OptimizationOrchestratorImpl;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * A class that orchestrates the overall optimization process for a Cruise job.
 */
@DefaultImplementation(OptimizationOrchestratorImpl.class)
public interface OptimizationOrchestrator {

  /**
   * Start the optimization.
   */
  void start();
}
