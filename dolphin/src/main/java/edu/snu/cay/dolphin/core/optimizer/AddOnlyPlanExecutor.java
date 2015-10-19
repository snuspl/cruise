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

import edu.snu.cay.dolphin.core.DolphinDriver;
import edu.snu.cay.dolphin.core.avro.IterationInfo;
import edu.snu.cay.dolphin.core.sync.DriverSync;
import edu.snu.cay.services.em.driver.api.ElasticMemory;
import edu.snu.cay.services.em.plan.api.Plan;
import edu.snu.cay.services.em.plan.api.PlanExecutor;
import edu.snu.cay.services.em.plan.api.PlanResult;
import edu.snu.cay.services.em.plan.impl.PlanResultImpl;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A Plan Executor that only executes the "add" portion of a Plan.
 * It immediately adds Evaluators with the appropriate contexts via ElasticMemory.
 * It makes use of DriverSync to guarantee that Task submission happens while the ControllerTask
 * is paused. This ensures that GroupCommunication Topologies are correctly synchronized.
 *
 * This Executor should eventually be replaced with a more complete executor.
 */
public final class AddOnlyPlanExecutor implements PlanExecutor {
  private static final Logger LOG = Logger.getLogger(AddOnlyPlanExecutor.class.getName());
  private final ElasticMemory elasticMemory;
  private final DriverSync driverSync;
  private final InjectionFuture<DolphinDriver> dolphinDriver;

  private final ExecutorService mainExecutor = Executors.newSingleThreadExecutor();

  private ExecutingPlan executingPlan;

  @Inject
  private AddOnlyPlanExecutor(final ElasticMemory elasticMemory,
                              final DriverSync driverSync,
                              final InjectionFuture<DolphinDriver> dolphinDriver) {
    this.elasticMemory = elasticMemory;
    this.driverSync = driverSync;
    this.dolphinDriver = dolphinDriver;
  }

  @Override
  public Future<PlanResult> execute(final Plan plan) {
    return mainExecutor.submit(new Callable<PlanResult>() {
      /**
       * Immediately add Evaluators via ElasticMemory.
       * Once Contexts are ready, the Sync protocol is invoked with the PauseHandler that executes task submission.
       */
      @Override
      public PlanResult call() throws Exception {
        executingPlan = new ExecutingPlan(plan, dolphinDriver.get());

        if (plan.getEvaluatorsToAdd().isEmpty()) {
          return new PlanResultImpl();
        }

        for (final String evaluatorToAdd : plan.getEvaluatorsToAdd()) {
          LOG.log(Level.INFO, "Add new evaluator {0}", evaluatorToAdd);
          elasticMemory.add(1, 512, 1, new ContextActiveHandler());
        }
        executingPlan.awaitActiveContexts();

        LOG.log(Level.INFO, "All evaluators were added, will submit evaluators on pause.");
        driverSync.execute(new PauseHandler(executingPlan), new PauseFailedHandler(executingPlan));

        executingPlan = null;
        return new PlanResultImpl();
      }
    });
  }

  @Override
  public void onRunningTask(final RunningTask task) {
    if (executingPlan == null) {
      return;
    }
    executingPlan.onRunningTask(task);
  }

  /**
   * This handler is registered as a callback to ElasticMemory.add().
   */
  private final class ContextActiveHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext context) {
      LOG.log(Level.INFO, "Received new context {0}. Submitting task.", context);
      if (executingPlan == null) {
        throw new RuntimeException("ActiveContext " + context + " received, but no executingPlan available.");
      }

      LOG.log(Level.INFO, "Task queued.");
      executingPlan.onActiveContext(context);
    }
  }

  /**
   * Calls the relevant executingPlan methods to submit tasks, wait for the running tasks,
   * and return.
   */
  private final class PauseHandler implements EventHandler<IterationInfo> {
    private final ExecutingPlan executingPlan;
    
    PauseHandler(final ExecutingPlan executingPlan) {
      this.executingPlan = executingPlan;
    }
    
    @Override
    public void onNext(final IterationInfo iterationInfo) {
      executingPlan.onPaused(iterationInfo);

      LOG.log(Level.INFO, "All tasks were submitted, waiting for RunningTasks.");
      final Collection<RunningTask> addedTasks = executingPlan.awaitRunningTasks();

      for (final RunningTask task : addedTasks) {
        LOG.log(Level.INFO, "Newly added task {0}", task);
      }

      LOG.log(Level.INFO, "All tasks were added, returning.");
    }
  }

  /**
   * Calls the relevant executingPlan methods to close any outstanding contexts.
   */
  private final class PauseFailedHandler implements EventHandler<Object> {
    private final ExecutingPlan executingPlan;

    PauseFailedHandler(final ExecutingPlan executingPlan) {
      this.executingPlan = executingPlan;
    }

    @Override
    public void onNext(final Object value) {
      executingPlan.onCancelled();
    }
  }

  /**
   * Encapsulates a single executing plan. The executing plan has a couple of barriers implemented as latches:
   * 1. Wait until all Contexts have been allocated as ActiveContexts.
   * 2. Wait until all Tasks submissions have completed as RunningTasks.
   */
  private static final class ExecutingPlan {
    private final ConcurrentMap<String, ActiveContext> pendingTaskSubmissions;
    private final List<ActiveContext> activeContexts;
    private final ConcurrentMap<String, RunningTask> runningTasks;
    private final CountDownLatch activeContextLatch;
    private final CountDownLatch runningTaskLatch;
    private final DolphinDriver dolphinDriver;

    private ExecutingPlan(final Plan plan, final DolphinDriver dolphinDriver) {
      this.pendingTaskSubmissions = new ConcurrentHashMap<>();
      this.activeContexts = new ArrayList<>();
      this.runningTasks = new ConcurrentHashMap<>();
      this.activeContextLatch = new CountDownLatch(plan.getEvaluatorsToAdd().size());
      this.runningTaskLatch = new CountDownLatch(plan.getEvaluatorsToAdd().size());
      this.dolphinDriver = dolphinDriver;
    }

    public void awaitActiveContexts() throws InterruptedException {
      activeContextLatch.await();
      activeContexts.addAll(pendingTaskSubmissions.values());
    }

    public void onActiveContext(final ActiveContext context) {
      pendingTaskSubmissions.put(context.getId(), context);
      activeContextLatch.countDown();
    }

    public void onPaused(final IterationInfo iterationInfo) {
      for (final ActiveContext context : activeContexts) {
        dolphinDriver.submitTask(context, iterationInfo);
      }
    }

    public Collection<RunningTask> awaitRunningTasks() {
      try {
        runningTaskLatch.await();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      return runningTasks.values();
    }

    public void onRunningTask(final RunningTask task) {
      final ActiveContext context = pendingTaskSubmissions.get(task.getActiveContext().getId());
      if (context != null) {
        runningTasks.put(task.getId(), task);
        runningTaskLatch.countDown();
      }
    }

    public void onCancelled() {
      LOG.log(Level.INFO, "Closing all outstanding contexts on cancel.");
      for (final ActiveContext context : activeContexts) {
        context.close();
      }
    }
  }
}
