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

import edu.snu.cay.dolphin.async.optimizer.parameters.DelayAfterOptimizationMs;
import edu.snu.cay.common.param.Parameters;
import edu.snu.cay.services.em.optimizer.api.EvaluatorParameters;
import edu.snu.cay.services.em.optimizer.api.Optimizer;
import edu.snu.cay.services.em.plan.api.Plan;
import edu.snu.cay.services.em.plan.api.PlanExecutor;
import edu.snu.cay.services.em.plan.api.PlanResult;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Orchestrates the Optimization in Dolphin Async.
 */
public final class OptimizationOrchestrator {
  private static final Logger LOG = Logger.getLogger(OptimizationOrchestrator.class.getName());

  public static final String NAMESPACE_SERVER = "SERVER";
  public static final String NAMESPACE_WORKER = "WORKER";

  private final Optimizer optimizer;
  private final PlanExecutor planExecutor;
  private final MetricsHub metricsHub;
  private final AtomicBoolean isPlanExecuting = new AtomicBoolean(false);

  private final ExecutorService optimizationThreadPool = Executors.newSingleThreadExecutor();

  /**
   * A delay after completion of optimization to wait the system to be stable.
   */
  private final long delayAfterOptimizationMs;

  private final int maxNumEvals;

  @Inject
  private OptimizationOrchestrator(final Optimizer optimizer,
                                   final PlanExecutor planExecutor,
                                   final MetricsHub metricsHub,
                                   @Parameter(DelayAfterOptimizationMs.class) final long delayAfterOptimizationMs,
                                   @Parameter(Parameters.LocalRuntimeMaxNumEvaluators.class) final int maxNumEvals) {
    this.optimizer = optimizer;
    this.planExecutor = planExecutor;
    this.metricsHub = metricsHub;
    this.delayAfterOptimizationMs = delayAfterOptimizationMs;
    this.maxNumEvals = maxNumEvals;
  }

  public void run() {
    if (!isPlanExecuting.compareAndSet(false, true)) {
      LOG.log(Level.INFO, "Skipping Optimization, because it is already under optimization process");
      return;
    }

    optimizationThreadPool.submit(new Runnable() {
      @Override
      public void run() {
        LOG.log(Level.INFO, "Optimization start. Start calculating the optimal plan");

        final Map<String, List<EvaluatorParameters>> evaluatorParameters = new HashMap<>(2);
        evaluatorParameters.put(NAMESPACE_SERVER, metricsHub.drainServerMetrics());
        evaluatorParameters.put(NAMESPACE_WORKER, metricsHub.drainWorkerMetrics());

        final Plan plan = optimizer.optimize(evaluatorParameters, maxNumEvals);
        LOG.log(Level.INFO, "Calculating the optimal plan is finished. Start executing plan: {0}", plan);

        final Future<PlanResult> planExecutionResultFuture = planExecutor.execute(plan);
        try {
          final PlanResult planResult = planExecutionResultFuture.get();
          LOG.log(Level.INFO, "Result of plan execution: {0}", planResult);

          // TODO #343: Optimization trigger component
          Thread.sleep(delayAfterOptimizationMs); // sleep for the system to be stable
        } catch (final InterruptedException | ExecutionException e) {
          LOG.log(Level.WARNING, "Exception while waiting for the plan execution to be completed", e);
        }

        // make another optimization can go after the optimization is completely finished
        isPlanExecuting.set(false);
      }
    });
  }

  /**
   * Checks whether the optimization is being performed, specifically whether the
   * plan is being executed.
   * @return True if the generated plan is on execution.
   */
  public boolean isPlanExecuting() {
    return isPlanExecuting.get();
  }
}
