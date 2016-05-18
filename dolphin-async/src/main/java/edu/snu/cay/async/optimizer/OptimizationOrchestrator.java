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
package edu.snu.cay.async.optimizer;

import edu.snu.cay.common.param.Parameters;
import edu.snu.cay.services.em.driver.api.ElasticMemory;
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
  public static final String DATA_TYPE_SERVER = "SERVER_DATA"; // DynamicPS should use this type.
  public static final String DATA_TYPE_WORKER = "WORKER_DATA"; // All Workers should use this type.

  private final Optimizer optimizer;
  private final PlanExecutor planExecutor;
  private final AtomicBoolean isPlanExecuting = new AtomicBoolean(false);

  private final ExecutorService optimizationThreadPool = Executors.newSingleThreadExecutor();

  private ElasticMemory serverEM;
  private ElasticMemory workerEM;

  private final int maxNumEvals;

  @Inject
  private OptimizationOrchestrator(final Optimizer optimizer,
                                   final PlanExecutor planExecutor,
                                   @Parameter(ServerEM.class) final ElasticMemory serverEM,
                                   @Parameter(WorkerEM.class) final ElasticMemory workerEM,
                                   @Parameter(Parameters.LocalRuntimeMaxNumEvaluators.class) final int maxNumEvals) {
    this.optimizer = optimizer;
    this.planExecutor = planExecutor;
    this.serverEM = serverEM;
    this.workerEM = workerEM;
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
        final Collection<EvaluatorParameters> serverEvalParams = serverEM.generateEvalParams(DATA_TYPE_SERVER).values();
        final Collection<EvaluatorParameters> workerEvalParams = workerEM.generateEvalParams(DATA_TYPE_WORKER).values();

        final Map<String, List<EvaluatorParameters>> evaluatorParameters = new HashMap<>(2);
        evaluatorParameters.put(NAMESPACE_SERVER, new ArrayList<>(serverEvalParams));
        evaluatorParameters.put(NAMESPACE_WORKER, new ArrayList<>(workerEvalParams));

        final Plan plan = optimizer.optimize(evaluatorParameters, maxNumEvals);
        LOG.log(Level.INFO, "Calculating the optimal plan is finished. Start executing plan: {0}", plan);

        final Future<PlanResult> planExecutionResultFuture = planExecutor.execute(plan);
        try {
          final PlanResult planResult = planExecutionResultFuture.get();
          LOG.log(Level.INFO, "Result of plan execution: {0}", planResult);

          Thread.sleep(10000); // sleep for the system to be stable
        } catch (final InterruptedException | ExecutionException e) {
          LOG.log(Level.WARNING, "Exception while waiting for the plan execution to be completed");
        }

        // make another optimization can go after the optimization is completely finished
        isPlanExecuting.set(false);
      }
    });
  }

  public boolean isPlanExecuting() {
    return isPlanExecuting.get();
  }
}
