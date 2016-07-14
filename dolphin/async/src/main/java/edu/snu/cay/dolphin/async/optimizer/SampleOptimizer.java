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
package edu.snu.cay.dolphin.async.optimizer;

import edu.snu.cay.services.em.optimizer.api.EvaluatorParameters;
import edu.snu.cay.services.em.optimizer.api.Optimizer;
import edu.snu.cay.services.em.optimizer.impl.DataInfoImpl;
import edu.snu.cay.services.em.plan.api.Plan;
import edu.snu.cay.services.em.plan.impl.PlanImpl;
import edu.snu.cay.services.em.plan.impl.TransferStepImpl;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A sample optimizer that creates plan one of below two.
 * 1) Add one server and Delete one worker.
 * 2) Delete one server and Add one worker.
 *
 * Of course each Delete and Add accompany Move in its own namespace.
 * These plans never changes the total number of evaluators participating in the job.
 */
public final class SampleOptimizer implements Optimizer {
  private static final Logger LOG = Logger.getLogger(SampleOptimizer.class.getName());

  private static final int MAX_CALLS_TO_MAKE = 3;
  private static final String EVAL_PREFIX = "NEW-";

  private AtomicInteger planContextIdCounter = new AtomicInteger(0);

  private int callsMade = 0;

  @Inject
  private SampleOptimizer() {

  }

  /**
   * Builds a plan that deletes one eval from {@code srcNamespace} and adds one to {@code destNamespace},
   * based on the {@code evalParamsMap}.
   * @param srcNamespace a source namespace
   * @param destNamespace a destination namespace
   * @param evalParamsMap all currently active evaluators and their parameters associated with the namespace
   */
  private Plan getPlanSwapEvalBetweenNamespaces(final String srcNamespace,
                                                final String destNamespace,
                                                final Map<String, List<EvaluatorParameters>> evalParamsMap) {
    final PlanImpl.Builder planBuilder = PlanImpl.newBuilder();

    // 1. source namespace:  find one eval to delete and one eval to move data into it.
    String srcNSEvalToDel = null;
    int srcNSBlocksToMove = 0;

    final List<EvaluatorParameters> srcNSEvalParamsList = evalParamsMap.get(srcNamespace);
    if (srcNSEvalParamsList == null) {
      LOG.log(Level.INFO, "There's no parameters for source namespace: {0}", srcNamespace);
      return planBuilder.build();
    }
    for (final EvaluatorParameters srcNSEvalParams : srcNSEvalParamsList) {
      final int numBlocks = srcNSEvalParams.getDataInfo().getNumBlocks();
      if (numBlocks > 0) {
        srcNSEvalToDel = srcNSEvalParams.getId();
        srcNSBlocksToMove = numBlocks;
        srcNSEvalParamsList.remove(srcNSEvalParams);
        break;
      }
    }

    if (srcNSEvalToDel == null) {
      LOG.warning("Fail to find eval with some data");
      return planBuilder.build();
    }

    // find eval to which move the data from eval to be deleted
    final String srcNSEvalToMove;
    if (srcNSEvalParamsList.isEmpty()) {
      LOG.warning("Fail to find eval to move data from eval to be deleted");
      return planBuilder.build();
    }
    srcNSEvalToMove = srcNSEvalParamsList.get(0).getId();


    // 2. destination namespace: find one eval to move data from it
    String destNSEvalToMove = null;
    int destNSBlocksToMove = 0;

    final List<EvaluatorParameters> destNSEvalParamsList = evalParamsMap.get(destNamespace);
    if (destNSEvalParamsList == null) {
      LOG.log(Level.INFO, "There's no parameters for destination namespace: {0}", destNamespace);
      return planBuilder.build();
    }
    for (final EvaluatorParameters destNSEvalParams : destNSEvalParamsList) {
      final int numBlocks = destNSEvalParams.getDataInfo().getNumBlocks();
      if (numBlocks > 1) {
        destNSEvalToMove = destNSEvalParams.getId();
        destNSBlocksToMove = numBlocks / 2;
        break;
      }
    }

    if (destNSEvalToMove == null) {
      LOG.warning("Fail to find server to move data from it");
      return planBuilder.build();
    }

    final String destNSEvalToAdd = EVAL_PREFIX + planContextIdCounter.getAndIncrement();

    return planBuilder
        .addEvaluatorToDelete(srcNamespace, srcNSEvalToDel)
        .addTransferStep(srcNamespace,
            new TransferStepImpl(srcNSEvalToDel, srcNSEvalToMove, new DataInfoImpl(srcNSBlocksToMove)))
        .addEvaluatorToAdd(destNamespace, destNSEvalToAdd)
        .addTransferStep(destNamespace,
            new TransferStepImpl(destNSEvalToMove, destNSEvalToAdd, new DataInfoImpl(destNSBlocksToMove)))
        .build();
  }

  /**
   * It does not use {@code availableEvaluators}, because this optimizer always retain
   * the total number of evaluators to use.
   */
  @Override
  public Plan optimize(final Map<String, List<EvaluatorParameters>> evalParamsMap, final int availableEvaluators) {
    if (callsMade == MAX_CALLS_TO_MAKE || evalParamsMap.isEmpty()) {
      return PlanImpl.newBuilder().build();
    }

    final Plan plan;

    if (callsMade % 2 == 0) {
      plan = getPlanSwapEvalBetweenNamespaces(OptimizationOrchestrator.NAMESPACE_WORKER,
          OptimizationOrchestrator.NAMESPACE_SERVER,
          evalParamsMap);
    } else {
      plan = getPlanSwapEvalBetweenNamespaces(OptimizationOrchestrator.NAMESPACE_SERVER,
          OptimizationOrchestrator.NAMESPACE_WORKER,
          evalParamsMap);
    }

    callsMade++;
    return plan;
  }
}
