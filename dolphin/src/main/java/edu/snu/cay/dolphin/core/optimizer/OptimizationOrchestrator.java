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
import edu.snu.cay.services.em.plan.impl.PlanResultImpl;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
  static final Future<PlanResult> INITIAL_RESULT_FUTURE = new InitialResult();

  private final Optimizer optimizer;
  private final PlanExecutor planExecutor;

  private final Map<String, MetricsReceiver> iterationIdToMetrics;

  private Future<PlanResult> planExecutionResult = INITIAL_RESULT_FUTURE;

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

  public void run(final Map<String, List<DataInfo>> dataInfos,
                  final Map<String, Map<String, Double>> computeMetrics,
                  final String controllerId,
                  final Map<String, Double> controllerMetrics) {
    LOG.log(Level.INFO, "Beginning migration");
    final Plan plan = optimizer.optimize(
        getNodeParameters(dataInfos, computeMetrics),
        getAvailableEvaluators(computeMetrics.size()));

    if (planExecutionResult.isDone()) {
      try {
        LOG.log(Level.INFO, "Previous result: " + planExecutionResult.get());
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      } catch (final ExecutionException e) {
        throw new RuntimeException(e);
      }
      planExecutionResult = planExecutor.execute(plan);
      try {
        // TODO #96: Enable background migration
        LOG.log(Level.INFO, "Blocking until the migration is done.");
        final PlanResult result = planExecutionResult.get();
        LOG.log(Level.INFO, "Current result: " + result);
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      } catch (final ExecutionException e) {
        throw new RuntimeException(e);
      }
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
  private Collection<EvaluatorParameters> getNodeParameters(final Map<String, List<DataInfo>> dataInfos,
                                                            final Map<String, Map<String, Double>> metrics) {
    final List<EvaluatorParameters> evaluatorParametersList = new ArrayList<>(dataInfos.size());
    for (final String computeId : dataInfos.keySet()) {
      evaluatorParametersList.add(new EvaluatorParametersImpl(computeId, dataInfos.get(computeId)));
    }
    return evaluatorParametersList;
  }

  /**
   * A {@code Future<PlanResult>} to be used as a singleton for an initially assigned result
   * (i.e., in place of null). The Future is initialized as done, and returns a singleton
   * PlanResult.
   */
  private static final class InitialResult implements Future<PlanResult> {
    private static final PlanResult INITIAL_RESULT = new PlanResultImpl();

    @Override
    public boolean cancel(final boolean mayInterruptIfRunning) {
      return false;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return true;
    }

    @Override
    public PlanResult get() throws InterruptedException, ExecutionException {
      return INITIAL_RESULT;
    }

    @Override
    public PlanResult get(final long timeout, final TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      return INITIAL_RESULT;
    }
  }
}
