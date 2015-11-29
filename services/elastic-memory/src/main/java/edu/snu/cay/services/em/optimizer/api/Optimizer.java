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
package edu.snu.cay.services.em.optimizer.api;

import edu.snu.cay.services.em.plan.api.Plan;

import java.util.Collection;
import java.util.Map;

/**
 * Given the current state of evaluators (as parameters) and available resources (as the number of available
 * evaluators), the optimizer generates an optimized plan.
 */
public interface Optimizer {
  /**
   * @param activeEvaluators all currently active evaluators and their parameters, excluding the controller task
   * @param availableEvaluators the total number of evaluators available for optimization, excluding the controller task
   *     If availableEvaluators < activeEvaluators.size(), the optimized plan must delete evaluators.
   *     If availableEvaluators > activeEvaluators.size(), the optimized plan may add evaluators.
   * @param ctrlTaskMetrics metrics collected by the controller task
   * @return the optimized plan
   */
  Plan optimize(Collection<EvaluatorParameters> activeEvaluators,
                int availableEvaluators,
                Map<String, Double> ctrlTaskMetrics);
}
