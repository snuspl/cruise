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

import edu.snu.cay.services.em.driver.api.ElasticMemory;
import edu.snu.cay.services.em.optimizer.api.Optimizer;
import edu.snu.cay.services.em.plan.api.Plan;
import edu.snu.cay.services.em.plan.api.PlanExecutor;
import edu.snu.cay.services.em.plan.api.PlanResult;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Orchestrates the Optimization in Dolphin Async.
 */
public final class OptimizationOrchestrator {
  private static final Logger LOG = Logger.getLogger(OptimizationOrchestrator.class.getName());

  private final Optimizer optimizer;
  private final PlanExecutor planExecutor;
  private final AtomicBoolean generatingOptimizationPlan = new AtomicBoolean(false);

  private final ExecutorService optimizationThreadPool = Executors.newSingleThreadExecutor();

  /**
   * The {@link Future} returned from the most recent {@code optimizationThreadPool.submit(Runnable)} call.
   * Note that this does not necessarily refer to a thread that is doing actual optimization
   * ({@link Optimizer#optimize(java.util.Collection, int)}).
   * This field is currently being used only for testing purposes.
   */
  private Future optimizationAttemptResult;

  private Future<PlanResult> planExecutionResult;
  private boolean planExecuting;

  private ElasticMemory serverEM;
  private ElasticMemory workerEM;

  @Inject
  OptimizationOrchestrator(final Optimizer optimizer,
                           final PlanExecutor planExecutor,
                           @Parameter(ServerEM.class) final ElasticMemory serverEM,
                           @Parameter(WorkerEM.class) final ElasticMemory workerEM) {
    this.optimizer = optimizer;
    this.planExecutor = planExecutor;
    this.serverEM = serverEM;
    this.workerEM = workerEM;
  }

  // TODO #00: When to trigger the optimization?
  public void run() {
    if (isPlanExecuting()) {
      LOG.log(Level.INFO, "Skipping Optimization, because some other thread is currently doing it");
      return;
    }

    if (!generatingOptimizationPlan.compareAndSet(false, true)) {
      LOG.log(Level.INFO, "Skipping Optimization, because some other thread is currently doing it");
      return;
    }

    optimizationAttemptResult = optimizationThreadPool.submit(new Runnable() {
      @Override
      public void run() {
        LOG.log(Level.INFO, "Optimization start.");
        logPreviousResult();
        serverEM.generateEvalParams();
        workerEM.generateEvalParams();

        final Plan plan = optimizer.optimize(null, 0);

        LOG.log(Level.INFO, "Optimization complete. Executing plan: {0}", plan);

        planExecutionResult = planExecutor.execute(plan);
        generatingOptimizationPlan.set(false);
      }
    });
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

}
