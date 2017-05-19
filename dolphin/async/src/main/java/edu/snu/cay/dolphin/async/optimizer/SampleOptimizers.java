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
package edu.snu.cay.dolphin.async.optimizer;

import edu.snu.cay.dolphin.async.optimizer.api.EvaluatorParameters;
import edu.snu.cay.dolphin.async.optimizer.api.Optimizer;
import edu.snu.cay.dolphin.async.optimizer.impl.DataInfoImpl;
import edu.snu.cay.dolphin.async.optimizer.parameters.Constants;
import edu.snu.cay.dolphin.async.plan.PlanImpl;
import edu.snu.cay.dolphin.async.plan.api.Plan;
import edu.snu.cay.dolphin.async.plan.api.TransferStep;
import edu.snu.cay.dolphin.async.plan.impl.TransferStepImpl;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A collection of sample optimizers.
 * It can be used for testing purpose.
 */
public final class SampleOptimizers {
  private static final Logger LOG = Logger.getLogger(SampleOptimizers.class.getName());

  /**
   * The maximum limit for each sample optimizer to be called.
   */
  public static final int MAX_CALLS_TO_MAKE = 3;

  /**
   * Prefix and index counter for evaluator id of Add operation.
   * Note that this id is temporary and used by PlanExecutor internally.
   * The actual Evaluator id is assigned by REEF, when it is allocated.
   */
  private static final String NEW_EVAL_PREFIX = "NEW";
  private static final AtomicInteger PLAN_CONTEXT_ID_COUNTER = new AtomicInteger(0);

  // utility class should not be instantiated
  private SampleOptimizers() {
  }

  /**
   * Get a plan that adds one evaluator and moves half block from other evaluator.
   * It adds new evaluator with empty blocks and then splits blocks in one existing evaluator
   * with the most blocks to the new one.
   * @param namespace a namespace
   * @param evalParams a list of evaluator parameters
   * @return a plan
   */
  private static Plan getAddOnePlan(final String namespace, final List<EvaluatorParameters> evalParams) {
    final PlanImpl.Builder planBuilder = PlanImpl.newBuilder();

    final String evalIdToAdd = NEW_EVAL_PREFIX + PLAN_CONTEXT_ID_COUNTER.getAndIncrement();
    planBuilder.addEvaluatorToAdd(namespace, evalIdToAdd);

    // sort evaluators by the number of blocks in descending order, to select evaluators with the most blocks
    Collections.sort(evalParams,
        (o1, o2) -> o2.getDataInfo().getNumBlocks() - o1.getDataInfo().getNumBlocks());

    if (!evalParams.isEmpty()) {
      final EvaluatorParameters srcEvaluator = evalParams.get(0);

      if (srcEvaluator.getDataInfo().getNumBlocks() > 0) {
        final int numBlocksToMove = srcEvaluator.getDataInfo().getNumBlocks() / 2; // choose half the blocks

        final TransferStep transferStep = new TransferStepImpl(
            srcEvaluator.getId(), evalIdToAdd, new DataInfoImpl(numBlocksToMove));

        planBuilder.addTransferStep(namespace, transferStep);

      } else {
        // no evaluator has data; a new evaluator will also have no data to work on
        LOG.log(Level.WARNING, "Cannot choose source evaluator to move its data to new evaluator in {0}", namespace);
      }
    }

    return planBuilder.build();
  }

  /**
   * An Optimizer that simply adds one new server Evaluator for each optimize call.
   * It then moves half the blocks from the Evaluator with most blocks to the new Evaluator.
   * It runs until {@link #MAX_CALLS_TO_MAKE} is reached.
   * Note that this optimizer does not care about the resource constraint.
   */
  public static final class AddOneServerOptimizer implements Optimizer {
    private int callsMade = 0;

    @Inject
    private AddOneServerOptimizer() {
    }

    @Override
    public Plan optimize(final Map<String, List<EvaluatorParameters>> evalParamsMap, final int availableEvaluators,
                         final Map<String, Double> optimizerModelParamsMap) {
      if (callsMade == MAX_CALLS_TO_MAKE || evalParamsMap.isEmpty()) {
        return PlanImpl.newBuilder().build();
      }

      final List<EvaluatorParameters> serverEvalParams = evalParamsMap.get(Constants.NAMESPACE_SERVER);

      final Plan plan = getAddOnePlan(Constants.NAMESPACE_SERVER, serverEvalParams);

      callsMade++;

      return plan;
    }
  }

  /**
   * An Optimizer that simply adds one new worker Evaluator for each optimize call.
   * It then moves half the blocks from the Evaluator with most blocks to the new Evaluator.
   * It runs until {@link #MAX_CALLS_TO_MAKE} is reached.
   * Note that this optimizer does not care about the resource constraint.
   */
  public static final class AddOneWorkerOptimizer implements Optimizer {
    private int callsMade = 0;

    @Inject
    private AddOneWorkerOptimizer() {
    }

    @Override
    public Plan optimize(final Map<String, List<EvaluatorParameters>> evalParamsMap, final int availableEvaluators,
                         final Map<String, Double> optimizerModelParamsMap) {
      if (callsMade == MAX_CALLS_TO_MAKE || evalParamsMap.isEmpty()) {
        return PlanImpl.newBuilder().build();
      }

      final List<EvaluatorParameters> workerEvalParams = evalParamsMap.get(Constants.NAMESPACE_WORKER);

      final Plan plan = getAddOnePlan(Constants.NAMESPACE_WORKER, workerEvalParams);

      callsMade++;

      return plan;
    }
  }

  /**
   * Get a plan that deletes one evaluator and moves all the blocks to other evaluator.
   * It merges the blocks in two evaluators with the least blocks into one evaluator that has more blocks.
   * @param namespace a namespace
   * @param evalParams a list of evaluator parameters
   * @return a plan
   */
  private static Plan getDeleteOnePlan(final String namespace, final List<EvaluatorParameters> evalParams) {
    final PlanImpl.Builder planBuilder = PlanImpl.newBuilder();

    if (evalParams.size() < 2) {
      LOG.log(Level.WARNING, "Cannot delete, because not enough evaluators in {0}", namespace);
      return planBuilder.build();
    }

    // sort evaluators by the number of blocks in ascending order, to select evaluators with the least blocks
    Collections.sort(evalParams,
        (o1, o2) -> o1.getDataInfo().getNumBlocks() - o2.getDataInfo().getNumBlocks());

    final EvaluatorParameters evalToDel = evalParams.get(0);
    planBuilder.addEvaluatorToDelete(namespace, evalToDel.getId());

    if (evalToDel.getDataInfo().getNumBlocks() > 0) {
      final EvaluatorParameters dstEvaluator = evalParams.get(1);

      // add a transfer step
      final int numBlocksInEvalToBeDeleted = evalToDel.getDataInfo().getNumBlocks();
      final TransferStep transferStep = new TransferStepImpl(
          evalToDel.getId(),
          dstEvaluator.getId(),
          new DataInfoImpl(numBlocksInEvalToBeDeleted));

      planBuilder.addTransferStep(namespace, transferStep);

    } else {
      LOG.log(Level.INFO, "Delete empty evaluator in {0}", namespace);
    }

    return planBuilder.build();
  }

  /**
   * An Optimizer that simply deletes one server Evaluator for each optimize call.
   * The plan transfers all the blocks from the soon-to-be-deleted Evaluator to a random Evaluator,
   * then removes the Evaluator.
   * It runs until {@link #MAX_CALLS_TO_MAKE} is reached.
   */
  public static final class DeleteOneServerOptimizer implements Optimizer {
    private int callsMade = 0;

    @Inject
    private DeleteOneServerOptimizer() {
    }

    @Override
    public Plan optimize(final Map<String, List<EvaluatorParameters>> evalParamsMap, final int availableEvaluators,
                         final Map<String, Double> optimizerModelParamsMap) {
      if (callsMade == MAX_CALLS_TO_MAKE || evalParamsMap.isEmpty()) {
        return PlanImpl.newBuilder().build();
      }

      final List<EvaluatorParameters> serverEvalParams = evalParamsMap.get(Constants.NAMESPACE_SERVER);

      final Plan plan = getDeleteOnePlan(Constants.NAMESPACE_SERVER, serverEvalParams);

      callsMade++;

      return plan;
    }
  }

  /**
   * An Optimizer that simply deletes one worker Evaluator for each optimize call.
   * The plan transfers all the blocks from the soon-to-be-deleted Evaluator to a random Evaluator,
   * then removes the Evaluator.
   * It runs until {@link #MAX_CALLS_TO_MAKE} is reached.
   */
  public static final class DeleteOneWorkerOptimizer implements Optimizer {
    private int callsMade = 0;

    @Inject
    private DeleteOneWorkerOptimizer() {
    }

    @Override
    public Plan optimize(final Map<String, List<EvaluatorParameters>> evalParamsMap, final int availableEvaluators,
                         final Map<String, Double> optimizerModelParamsMap) {
      if (callsMade == MAX_CALLS_TO_MAKE || evalParamsMap.isEmpty()) {
        return PlanImpl.newBuilder().build();
      }

      final List<EvaluatorParameters> workerEvalParams = evalParamsMap.get(Constants.NAMESPACE_WORKER);

      final Plan plan = getDeleteOnePlan(Constants.NAMESPACE_WORKER, workerEvalParams);

      callsMade++;

      return plan;
    }
  }

  /**
   * An optimizer implementation that exchanges evaluators between server and worker namespaces.
   * More specifically, an evaluator is deleted from one namespace, and added on the other at a time.
   * Of course each Delete and Add accompany Move in its own namespace.
   * These plans never changes the total number of evaluators participating in the job.
   * The purpose of this optimizer is to consider where the amount of resources are constrained.
   */
  public static final class ExchangeOneOptimizer implements Optimizer {
    private int callsMade = 0;

    @Inject
    private ExchangeOneOptimizer() {
    }

    /**
     * Builds a plan that deletes one eval from {@code srcNamespace} and adds one to {@code destNamespace},
     * based on the {@code evalParamsMap}.
     *
     * @param srcNamespace  a source namespace
     * @param destNamespace a destination namespace
     * @param evalParamsMap all currently active evaluators and their parameters associated with the namespace
     */
    private Plan getPlanSwapEvalBetweenNamespaces(final String srcNamespace,
                                                  final String destNamespace,
                                                  final Map<String, List<EvaluatorParameters>> evalParamsMap,
                                                  final int availableEvaluators) {
      final PlanImpl.Builder planBuilder = PlanImpl.newBuilder();

      // 1. source namespace: find one eval to delete and one eval to move data into it.
      String srcNSEvalToDel = null;
      int srcNSBlocksToMove = 0;

      final List<EvaluatorParameters> srcNSEvalParamsList = evalParamsMap.get(srcNamespace);
      if (srcNSEvalParamsList == null) {
        LOG.log(Level.INFO, "There's no parameters for source namespace: {0}", srcNamespace);
        return planBuilder.build();
      }

      final List<EvaluatorParameters> destNSEvalParamsList = evalParamsMap.get(destNamespace);
      if (destNSEvalParamsList == null) {
        LOG.log(Level.INFO, "There's no parameters for destination namespace: {0}", destNamespace);
        return planBuilder.build();
      }

      final int numAvailableExtraEvals
          = availableEvaluators - (srcNSEvalParamsList.size() + destNSEvalParamsList.size());

      // pick the very first evaluator that has any data block in it to be the evaluator to be deleted
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

      // pick the next eval to move data into
      final String srcNSEvalToMove;
      if (srcNSEvalParamsList.isEmpty()) {
        LOG.warning("Fail to find eval to move data into");
        return planBuilder.build();
      }
      srcNSEvalToMove = srcNSEvalParamsList.get(0).getId();


      // 2. destination namespace: find one eval to move data from it
      String destNSEvalToMove = null;
      int destNSBlocksToMove = 0;

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

      final String destNSEvalToAdd = NEW_EVAL_PREFIX + PLAN_CONTEXT_ID_COUNTER.getAndIncrement();

      return planBuilder
          .setNumAvailableExtraEvaluators(numAvailableExtraEvals)
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
    public Plan optimize(final Map<String, List<EvaluatorParameters>> evalParamsMap, final int availableEvaluators,
                         final Map<String, Double> optimizerModelParamsMap) {
      if (callsMade == MAX_CALLS_TO_MAKE || evalParamsMap.isEmpty()) {
        return PlanImpl.newBuilder().build();
      }

      final Plan plan;

      if (callsMade % 2 == 0) {
        plan = getPlanSwapEvalBetweenNamespaces(Constants.NAMESPACE_WORKER, Constants.NAMESPACE_SERVER,
            evalParamsMap, availableEvaluators);
      } else {
        plan = getPlanSwapEvalBetweenNamespaces(Constants.NAMESPACE_SERVER, Constants.NAMESPACE_WORKER,
            evalParamsMap, availableEvaluators);
      }

      callsMade++;
      return plan;
    }
  }
}
