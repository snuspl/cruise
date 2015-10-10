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

import edu.snu.cay.services.em.optimizer.api.DataInfo;
import edu.snu.cay.services.em.optimizer.impl.EvaluatorParametersImpl;
import edu.snu.cay.services.em.plan.api.PlanExecutor;
import edu.snu.cay.services.em.optimizer.api.EvaluatorParameters;
import edu.snu.cay.services.em.optimizer.api.Optimizer;
import edu.snu.cay.services.em.plan.api.Plan;
import edu.snu.cay.services.em.plan.api.PlanResult;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Orchestrates EM optimization and plan execution within the Dolphin runtime.
 * The OptimizationOrchestrator keeps track of received messages, for each
 * (comm group, iteration) pair, by creating an instance of MetricsReceiver.
 * When all metrics are received for a (comm group, iteration), the optimizer is
 * called, and the resulting Plan is executed.
 *
 * TODO #96: Enable background migration
 * This orchestrator blocks while running a Plan execution. When background migration
 * is implemented, the orchestrator should run Plan execution in the background to hide the
 * Plan execution time.
 *
 */
public final class OptimizationOrchestrator {
  private static final Logger LOG = Logger.getLogger(OptimizationOrchestrator.class.getName());

  private final Optimizer optimizer;
  private final PlanExecutor planExecutor;

  private final Map<String, MetricsReceiver> iterationIdToMetrics;

  private Future<PlanResult> planExecutionResult;

  @Inject
  private OptimizationOrchestrator(final Optimizer optimizer,
                                   final PlanExecutor planExecutor) {
    this.optimizer = optimizer;
    this.planExecutor = planExecutor;

    this.iterationIdToMetrics = new HashMap<>();
  }

  public synchronized void receiveComputeMetrics(final String contextId,
                                                 final String groupName,
                                                 final int iteration,
                                                 final Map<String, Double> metrics,
                                                 final List<DataInfo> dataInfos) {
    getIterationMetrics(groupName, iteration).addCompute(contextId, metrics, dataInfos);
  }

  public synchronized void receiveControllerMetrics(final String contextId,
                                                    final String groupName,
                                                    final int iteration,
                                                    final Map<String, Double> metrics,
                                                    final int numComputeTasks) {
    getIterationMetrics(groupName, iteration).addController(contextId, metrics, numComputeTasks);
  }

  private MetricsReceiver getIterationMetrics(final String groupName, final int iteration) {
    final String iterationId = groupName + iteration;
    if (!iterationIdToMetrics.containsKey(iterationId)) {
      iterationIdToMetrics.put(iterationId, new MetricsReceiver(this));
    }
    return iterationIdToMetrics.get(iterationId);
  }

  /**
   * Runs the optimization: get an optimized Plan based on the current Evaluator parameters, then execute the plan.
   * Optimization is skipped if the previous optimization has not finished.
   * TODO #96: We block until the Plan execution completes. This will change when background migration is implemented.
   */
  public synchronized void run(final Map<String, List<DataInfo>> dataInfos,
                               final Map<String, Map<String, Double>> computeMetrics,
                               final String controllerId,
                               final Map<String, Double> controllerMetrics) {
    if (isPlanExecuting()) {
      LOG.log(Level.INFO, "Skipping Optimization, as the previous plan is still executing.");
      return;
    }

    LOG.log(Level.INFO, "Optimization start.");
    logPreviousResult();

    final Plan plan = optimizer.optimize(
        getEvaluatorParameters(dataInfos, computeMetrics, controllerId, controllerMetrics),
        getAvailableEvaluators(computeMetrics.size()));

    planExecutionResult = planExecutor.execute(plan);

    // TODO #96: Enable background migration
    blockUntilPlanIsExecuted();

    LOG.log(Level.INFO, "Optimization complete.");
  }

  private boolean isPlanExecuting() {
    return planExecutionResult != null && !planExecutionResult.isDone();
  }

  private void logPreviousResult() {
    if (planExecutionResult == null) {
      LOG.log(Level.INFO, "Initial optimization run.");
    } else {
      try {
        LOG.log(Level.INFO, "Previous result: {0}", planExecutionResult.get());
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      } catch (final ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void blockUntilPlanIsExecuted() {
    try {
      LOG.log(Level.INFO, "Blocking until current plan execution is done.");
      final PlanResult result = planExecutionResult.get();
      LOG.log(Level.INFO, "Current result: {0}", result);
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    } catch (final ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  Future<PlanResult> getPlanExecutionResult() {
    return planExecutionResult;
  }

  /**
   * Returns the number of available evaluators to be considered in optimization.
   * Currently, it just gives one more evaluator than currently used.
   * TODO #176: assign availableEvaluators depending on the resource situation
   * @return the number of available evaluators to be considered in optimization
   */
  private int getAvailableEvaluators(final int numEvaluators) {
    return numEvaluators + 1;
  }

  // TODO #55: Information needed for the mathematical optimization formulation should be added to EvaluatorParameters
  private Collection<EvaluatorParameters> getEvaluatorParameters(final Map<String, List<DataInfo>> dataInfos,
                                                                 final Map<String, Map<String, Double>> metrics,
                                                                 final String controllerId,
                                                                 final Map<String, Double> controllerMetrics) {
    final List<EvaluatorParameters> evaluatorParametersList = new ArrayList<>(dataInfos.size());
    for (final String computeId : dataInfos.keySet()) {
      evaluatorParametersList.add(
          new EvaluatorParametersImpl(computeId, dataInfos.get(computeId), metrics.get(computeId)));
    }
    return evaluatorParametersList;
  }
}
