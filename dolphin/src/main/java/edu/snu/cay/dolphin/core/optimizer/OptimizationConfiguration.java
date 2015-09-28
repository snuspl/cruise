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
package edu.snu.cay.dolphin.core.optimizer;

import edu.snu.cay.services.em.plan.api.PlanExecutor;
import edu.snu.cay.services.em.optimizer.api.Optimizer;
import edu.snu.cay.services.em.optimizer.impl.RandomOptimizer;
import edu.snu.cay.services.em.plan.impl.LoggingPlanExecutor;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;

/**
 * Provides convenience methods that configure Optimizers and Executors for Dolphin.
 */
public final class OptimizationConfiguration {
  private OptimizationConfiguration() {
  }

  /**
   * @return a configuration with RandomOptimizer and LoggingPlanExecutor
   */
  public static Configuration getRandomOptimizerConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(Optimizer.class, RandomOptimizer.class)
        .bindImplementation(PlanExecutor.class, LoggingPlanExecutor.class)
        .build();
  }
}
