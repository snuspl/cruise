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
package edu.snu.spl.cruise.ps.optimizer.api;

import edu.snu.spl.cruise.ps.plan.api.Plan;

import java.util.List;
import java.util.Map;

/**
 * Given the current state of evaluators (as parameters) and available resources (as the number of available
 * evaluators), the optimizer generates an optimized plan.
 */
public interface Optimizer {
  /**
   * @param evalParamsMap all currently active evaluators and their parameters associated with the namespace.
   * @param availableEvaluators the total number of evaluators available for optimization.
   *     If availableEvaluators < activeEvaluators.size(), the optimized plan must delete evaluators.
   *     If availableEvaluators > activeEvaluators.size(), the optimized plan may add evaluators.
   * @return the optimized plan. An empty plan is returned if there is no reconfiguration to be conducted.
   */
  Plan optimize(Map<String, List<EvaluatorParameters>> evalParamsMap, int availableEvaluators,
                Map<String, Double> optimizerModelParamsMap);
}
