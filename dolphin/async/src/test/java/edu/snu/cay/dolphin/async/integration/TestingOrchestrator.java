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
package edu.snu.cay.dolphin.async.integration;

import edu.snu.cay.common.param.Parameters.LocalRuntimeMaxNumEvaluators;
import edu.snu.cay.dolphin.async.optimizer.*;
import edu.snu.cay.dolphin.async.optimizer.SampleOptimizers.*;
import edu.snu.cay.dolphin.async.optimizer.parameters.Constants;
import edu.snu.cay.services.em.driver.api.ElasticMemory;
import edu.snu.cay.services.em.optimizer.api.EvaluatorParameters;
import edu.snu.cay.services.em.optimizer.api.Optimizer;
import edu.snu.cay.services.em.plan.api.Plan;
import edu.snu.cay.services.em.plan.api.PlanExecutor;
import edu.snu.cay.services.em.plan.api.PlanResult;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Testing purpose implementation of {@link OptimizationOrchestrator}.
 * It invokes various kinds of optimizers to validate the correct working of reconfiguration.
 */
final class TestingOrchestrator implements OptimizationOrchestrator {
  private static final Logger LOG = Logger.getLogger(TestingOrchestrator.class.getName());

  private final PlanExecutor planExecutor;
  private final MetricsHub metricsHub;

  private final AtomicBoolean isPlanExecuting = new AtomicBoolean(false);

  private final int maxNumEvals;

  private final ElasticMemory workerEM;
  private final ElasticMemory serverEM;

  private final List<EvaluatorParameters> serverParameters = new ArrayList<>();
  private final List<EvaluatorParameters> workerParameters = new ArrayList<>();

  private final Optimizer[] optimizers;

  @Inject
  private TestingOrchestrator(final PlanExecutor planExecutor,
                              final MetricsHub metricsHub,
                              @Parameter(WorkerEM.class) final ElasticMemory workerEM,
                              @Parameter(ServerEM.class) final ElasticMemory serverEM,
                              @Parameter(LocalRuntimeMaxNumEvaluators.class) final int maxNumEvals,
                              final AsyncDolphinOptimizer asyncDolphinOptimizer,
                              final AddOneServerOptimizer addOneServerOptimizer,
                              final AddOneWorkerOptimizer addOneWorkerOptimizer,
                              final DeleteOneServerOptimizer deleteOneServerOptimizer,
                              final DeleteOneWorkerOptimizer deleteOneWorkerOptimizer,
                              final ExchangeOneOptimizer exchangeOneOptimizer) {
    this.planExecutor = planExecutor;
    this.metricsHub = metricsHub;
    this.workerEM = workerEM;
    this.serverEM = serverEM;
    this.maxNumEvals = maxNumEvals;

    this.optimizers = new Optimizer[]{
        deleteOneWorkerOptimizer, addOneWorkerOptimizer,
        deleteOneServerOptimizer, addOneServerOptimizer,
        exchangeOneOptimizer, asyncDolphinOptimizer};
  }

  private int callsMade = 0;

  @Override
  public void run() {
    final int optimizerIdx = callsMade;

    if (optimizerIdx > optimizers.length - 1) {
      LOG.log(Level.INFO, "Optimization testing is done for all sample optimizers");
      return;
    }

    // 1) Checks the metric state whether it's enough for the optimization.
    serverParameters.addAll(metricsHub.drainServerMetrics());
    workerParameters.addAll(metricsHub.drainWorkerMetrics());

    final int numServerMetricSources = getNumMetricSources(serverParameters);
    final int numWorkerMetricSources = getNumMetricSources(workerParameters);
    final int numRunningServers = getNumRunningInstances(serverEM);
    final int numRunningWorkers = getNumRunningInstances(workerEM);

    // Case1. If there are metrics from dead nodes
    if (numServerMetricSources > numRunningServers || numWorkerMetricSources > numRunningWorkers) {
      LOG.log(Level.INFO, "Skip this round, because the collected metrics include ones from dead nodes." +
              "The current metrics will be dumped to prevent them from being used in the next optimization try." +
              " Metrics from Servers: {0} / {1}, from Workers: {2} / {3}",
          new Object[]{numServerMetricSources, numRunningServers, numWorkerMetricSources, numRunningWorkers});
      // Dump all the collected metrics
      serverParameters.clear();
      workerParameters.clear();
      return;

      // Case2. If there are missing metrics
    } else if (numServerMetricSources < numRunningServers || numWorkerMetricSources < numRunningWorkers) {
      LOG.log(Level.INFO, "Skip this round, because there are missing metrics." +
              " The existing metrics will be kept and reused in the next optimization try." +
              " Metrics from Servers: {0} / {1}, from Workers: {2} / {3}",
          new Object[]{numServerMetricSources, numRunningServers, numWorkerMetricSources, numRunningWorkers});
      // Just return and wait for more metrics to be collected
      return;
    }

    isPlanExecuting.set(true);

    // 2) Process metrics before starting optimization
    final Map<String, List<EvaluatorParameters>> evaluatorParameters = new HashMap<>(2);

    // use only one latest metric of each evaluator
    final List<EvaluatorParameters> latestServerMetrics = filterLatestParams(serverParameters);
    final List<EvaluatorParameters> latestWorkerMetrics = filterLatestParams(workerParameters);
    serverParameters.clear();
    workerParameters.clear();

    evaluatorParameters.put(Constants.NAMESPACE_SERVER, latestServerMetrics);
    evaluatorParameters.put(Constants.NAMESPACE_WORKER, latestWorkerMetrics);

    // 3) run plan from optimizer
    final Optimizer optimizer = optimizers[optimizerIdx];
    executePlans(evaluatorParameters, optimizer);

    isPlanExecuting.set(false);

    callsMade++;
  }

  private void executePlans(final Map<String, List<EvaluatorParameters>> evalParams,
                            final Optimizer optimizer) {
    LOG.log(Level.INFO, "Calculate plan from {0}", optimizer.getClass().getName());
    final Plan plan = optimizer.optimize(evalParams, maxNumEvals);

    final Future<PlanResult> planResultFuture = planExecutor.execute(plan);
    try {
      final PlanResult result = planResultFuture.get();

      LOG.log(Level.INFO, "The number of executed ops: {0}", result.getNumExecutedOps());

      if (plan.getPlanSize() != result.getNumExecutedOps()) {
        throw new RuntimeException("The number of executed operations is different from expectation");
      }

    } catch (final InterruptedException | ExecutionException e) {
      LOG.log(Level.SEVERE, "Exception while executing plan", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isPlanExecuting() {
    return isPlanExecuting.get();
  }

  private int getNumRunningInstances(final ElasticMemory em) {
    return em.getStoreIdToBlockIds().size();
  }

  private int getNumMetricSources(final List<EvaluatorParameters> evalParams) {
    return (int) evalParams.stream()
        .map(EvaluatorParameters::getId)
        .distinct().count();
  }

  /**
   * Filter the Evaluator Parameters, so that there exist only one latest value per Evaluator.
   */
  private List<EvaluatorParameters> filterLatestParams(final List<EvaluatorParameters> params) {
    final Map<String, EvaluatorParameters> evalIdToParameters = new HashMap<>();
    final ListIterator<EvaluatorParameters> listIterator = params.listIterator(params.size());

    // iterate from the tail in order to save the latest params
    while (listIterator.hasPrevious()) {
      final EvaluatorParameters evalParams = listIterator.previous();
      evalIdToParameters.putIfAbsent(evalParams.getId(), evalParams);
    }

    return new ArrayList<>(evalIdToParameters.values());
  }
}
