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
package edu.snu.cay.dolphin.async.optimizer.impl;

import edu.snu.cay.dolphin.async.optimizer.api.EvaluatorParameters;
import edu.snu.cay.dolphin.async.optimizer.api.Optimizer;
import edu.snu.cay.dolphin.async.plan.impl.PlanImpl;
import edu.snu.cay.dolphin.async.plan.api.Plan;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;

/**
 * An Optimizer implementation that creates a plan with no operations.
 */
public final class EmptyPlanOptimizer implements Optimizer {

  @Inject
  private EmptyPlanOptimizer() {

  }

  @Override
  public Plan optimize(final Map<String, List<EvaluatorParameters>> evalParamsMap, final int availableEvaluators,
                       final Map<String, Double> optimizerModelParamsMap) {
    return PlanImpl.newBuilder().build();
  }
}
