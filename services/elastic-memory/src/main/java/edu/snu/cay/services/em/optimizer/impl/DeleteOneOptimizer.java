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

import edu.snu.cay.services.em.optimizer.api.EvaluatorParameters;
import edu.snu.cay.services.em.optimizer.api.Optimizer;
import edu.snu.cay.services.em.plan.api.Plan;
import edu.snu.cay.services.em.plan.api.TransferStep;
import edu.snu.cay.services.em.plan.impl.PlanImpl;
import edu.snu.cay.services.em.plan.impl.TransferStepImpl;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * An Optimizer that simply deletes one new Evaluator for each optimize call.
 * The plan transfers all the blocks from the soon-to-be-deleted Evaluator to a random Evaluator,
 * then removes the Evaluator.
 * It skips until callsToSkip is reached, after which it runs until maxCallsToMake is reached.
 *
 * This Optimizer can be used to drive DefaultPlanExecutor for testing purposes.
 */
public final class DeleteOneOptimizer implements Optimizer {
  private static final Logger LOG = Logger.getLogger(DeleteOneOptimizer.class.getName());

  private final int maxCallsToMake = 1;
  private int callsMade = 0;

  private final int callsToSkip = 0;
  private int callsSkipped = 0;

  @Inject
  private DeleteOneOptimizer() {
  }

  @Override
  public Plan optimize(final Map<String, List<EvaluatorParameters>> evalParamsMap, final int availableEvaluators) {
    if (callsSkipped < callsToSkip) {
      callsSkipped++;
      return PlanImpl.newBuilder().build();
    }

    if (callsMade == maxCallsToMake || evalParamsMap.isEmpty()) {
      return PlanImpl.newBuilder().build();
    }

    callsMade++;

    final PlanImpl.Builder planBuilder = PlanImpl.newBuilder();

    for (final Map.Entry<String, List<EvaluatorParameters>> entry : evalParamsMap.entrySet()) {
      final String namespace = entry.getKey();
      final List<EvaluatorParameters> evalParams = entry.getValue();

      if (evalParams.size() < 2) {
        LOG.log(Level.WARNING, "Cannot delete, because not enough evaluators in {0}", namespace);
        continue;
      }

      // make a new array to avoid the change in input
      final List<EvaluatorParameters> evaluators = new ArrayList<>(evalParams.size());
      // only add evaluators that have data to move
      evaluators.addAll(evalParams
          .stream()
          .filter(evaluator -> evaluator.getDataInfo().getNumBlocks() > 0)
          .collect(Collectors.toList()));

      // sort evaluators by ID, to select evaluators in a consistent order across job executions
      Collections.sort(evaluators, (o1, o2) -> o1.getId().compareTo(o2.getId()));

      final EvaluatorParameters evaluatorToDelete = evaluators.get(0);
      final EvaluatorParameters dstEvaluator = evaluators.get(1);

      planBuilder.addEvaluatorToDelete(namespace, evaluatorToDelete.getId());

      // add a transfer step, if evaluatorToDelete has blocks
      final int numBlocksInEvalToBeDeleted = evaluatorToDelete.getDataInfo().getNumBlocks();
      if (numBlocksInEvalToBeDeleted > 0) {
        final TransferStep transferStep = new TransferStepImpl(
            evaluatorToDelete.getId(),
            dstEvaluator.getId(),
            new DataInfoImpl(numBlocksInEvalToBeDeleted));
        planBuilder.addTransferStep(namespace, transferStep);
      }
    }

    return planBuilder.build();
  }
}
