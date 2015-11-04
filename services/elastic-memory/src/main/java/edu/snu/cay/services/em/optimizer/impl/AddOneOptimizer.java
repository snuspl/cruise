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
package edu.snu.cay.services.em.optimizer.impl;

import edu.snu.cay.services.em.optimizer.api.DataInfo;
import edu.snu.cay.services.em.optimizer.api.EvaluatorParameters;
import edu.snu.cay.services.em.optimizer.api.Optimizer;
import edu.snu.cay.services.em.plan.api.Plan;
import edu.snu.cay.services.em.plan.api.TransferStep;
import edu.snu.cay.services.em.plan.impl.PlanImpl;
import edu.snu.cay.services.em.plan.impl.TransferStepImpl;

import javax.inject.Inject;
import java.util.Collection;

/**
 * An Optimizer that simply adds one new Evaluator for each optimize call.
 * It then moves half the units from the first Evaluator to the new Evaluator.
 * It skips until callsToSkip is reached, after which it runs until maxCallsToMake is reached.
 *
 * This Optimizer can be used to drive AddOnlyPlanExecutor for testing purposes.
 */
public final class AddOneOptimizer implements Optimizer {
  private final int maxCallsToMake = 1;
  private int callsMade = 0;

  private final int callsToSkip = 0;
  private int callsSkipped = 0;

  @Inject
  private AddOneOptimizer() {
  }

  @Override
  public Plan optimize(final Collection<EvaluatorParameters> activeEvaluators, final int availableEvaluators) {
    if (callsSkipped < callsToSkip) {
      callsSkipped++;
      return PlanImpl.newBuilder().build();
    }

    if (callsMade == maxCallsToMake) {
      return PlanImpl.newBuilder().build();
    }

    final String evaluatorToAdd = "new-" + callsMade;
    callsMade++;

    final EvaluatorParameters srcEvaluator = activeEvaluators.iterator().next();
    final DataInfo srcDataInfo = srcEvaluator.getDataInfos().iterator().next();
    final int numUnitsToMove = srcDataInfo.getNumUnits() / 2;

    final TransferStep transferStep = new TransferStepImpl(
        srcEvaluator.getId(), evaluatorToAdd, new DataInfoImpl(srcDataInfo.getDataType(), numUnitsToMove));

    return PlanImpl.newBuilder()
        .addEvaluatorToAdd(evaluatorToAdd)
        .addTransferStep(transferStep)
        .build();
  }
}
