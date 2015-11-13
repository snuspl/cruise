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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * An Optimizer that simply deletes one new Evaluator for each optimize call.
 * The plan transfers all the units from the soon-to-be-deleted Evaluator to a random Evaluator,
 * then removes the Evaluator.
 * It skips until callsToSkip is reached, after which it runs until maxCallsToMake is reached.
 *
 * This Optimizer can be used to drive DefaultPlanExecutor for testing purposes.
 */
public final class DeleteOneOptimizer implements Optimizer {
  private final int maxCallsToMake = 1;
  private int callsMade = 0;

  private final int callsToSkip = 0;
  private int callsSkipped = 0;

  @Inject
  private DeleteOneOptimizer() {
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

    final Iterator<EvaluatorParameters> evaluatorIterator = activeEvaluators.iterator();
    final EvaluatorParameters evaluatorToDelete = evaluatorIterator.next();
    final EvaluatorParameters dstEvaluator = evaluatorIterator.next();

    final List<TransferStep> transferSteps = new ArrayList<>();
    for (final DataInfo dataInfo : evaluatorToDelete.getDataInfos()) {
      transferSteps.add(new TransferStepImpl(
          evaluatorToDelete.getId(),
          dstEvaluator.getId(),
          new DataInfoImpl(dataInfo.getDataType(), dataInfo.getNumUnits())));
    }

    callsMade++;

    return PlanImpl.newBuilder()
        .addEvaluatorToDelete(evaluatorToDelete.getId())
        .addTransferSteps(transferSteps)
        .build();
  }
}
