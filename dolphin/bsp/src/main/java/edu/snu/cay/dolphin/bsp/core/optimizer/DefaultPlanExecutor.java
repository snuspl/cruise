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
package edu.snu.cay.dolphin.bsp.core.optimizer;

import edu.snu.cay.common.param.Parameters.EvaluatorSize;
import edu.snu.cay.dolphin.bsp.core.sync.DriverSync;
import edu.snu.cay.dolphin.bsp.core.DolphinDriver;
import edu.snu.cay.dolphin.bsp.core.avro.IterationInfo;
import edu.snu.cay.services.em.avro.AvroElasticMemoryMessage;
import edu.snu.cay.services.em.avro.Result;
import edu.snu.cay.services.em.avro.ResultMsg;
import edu.snu.cay.services.em.driver.api.ElasticMemory;
import edu.snu.cay.services.em.plan.api.Plan;
import edu.snu.cay.services.em.plan.api.PlanExecutor;
import edu.snu.cay.services.em.plan.api.PlanResult;
import edu.snu.cay.services.em.plan.api.TransferStep;
import edu.snu.cay.services.em.plan.impl.PlanResultImpl;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.evaluator.context.parameters.ContextIdentifier;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
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

import static edu.snu.cay.dolphin.bsp.core.optimizer.OptimizationOrchestrator.NAMESPACE_DOLPHIN_BSP;

/**
 * A Plan Executor that executes a Plan.
 * It immediately adds Evaluators with the appropriate contexts via ElasticMemory.
 * It then makes use of DriverSync to guarantee the following happen between Task iterations.
 *   Execute moves according to the plan, by making use of the EM.move callbacks.
 *   Submit Tasks that are added to GroupCommunication Topologies (ensures GroupComm is correctly synchronized).
 *   Delete Evaluators that are no longer needed.
 *
 * TODO #90: This Executor needs to properly handle failure cases that are propagated here.
 */
public final class DefaultPlanExecutor implements PlanExecutor {
  private static final Logger LOG = Logger.getLogger(DefaultPlanExecutor.class.getName());
  private final ElasticMemory elasticMemory;
  private final DriverSync driverSync;
  private final InjectionFuture<DolphinDriver> dolphinDriver;
  private final int evalSize;

  private final ExecutorService mainExecutor = Executors.newSingleThreadExecutor();

  private ExecutingPlan executingPlan;

  @Inject
  private DefaultPlanExecutor(final ElasticMemory elasticMemory,
                              final DriverSync driverSync,
                              final InjectionFuture<DolphinDriver> dolphinDriver,
                              @Parameter(EvaluatorSize.class) final int evalSize) {
    this.elasticMemory = elasticMemory;
    this.driverSync = driverSync;
    this.dolphinDriver = dolphinDriver;
    this.evalSize = evalSize;
  }

  /**
   * Executes a plan using ElasticMemory and DriverSync.
   *
   * The main steps are as follows. Intermediate steps take place within respective handlers,
   * as summarized in {@link ExecutingPlan}.
   * 1. Create and assign an executingPlan
   * 2. Call ElasticMemory.add(), wait for active contexts
   * 3. Call ElasticMemory.move(), wait for data transfers to complete
   * 4. Call DriverSync.execute() which executes {@link SynchronizedExecutionHandler}, wait for execution to complete
   * 5. Clear executingPlan and return
   *
   * @param plan to execute
   * @return execution result
   */
  @Override
  public Future<PlanResult> execute(final Plan plan) {
    return mainExecutor.submit(new Callable<PlanResult>() {
      /**
       * Immediately add Evaluators via ElasticMemory.
       * Once Contexts are ready, the Sync protocol is invoked with the PauseHandler that executes task submission.
       */
      @Override
      public PlanResult call() throws Exception {
        if (plan.getEvaluatorsToAdd(NAMESPACE_DOLPHIN_BSP).isEmpty() &&
            plan.getTransferSteps(NAMESPACE_DOLPHIN_BSP).isEmpty() &&
            plan.getEvaluatorsToDelete(NAMESPACE_DOLPHIN_BSP).isEmpty()) {
          return new PlanResultImpl();
        }
        executingPlan = new ExecutingPlan(plan);

        final EventHandler<AllocatedEvaluator> evaluatorAllocatedHandler = new EvaluatorAllocatedHandler();
        final List<EventHandler<ActiveContext>> contextActiveHandlers = new ArrayList<>();
        contextActiveHandlers.add(new ContextActiveHandler());
        for (final String evaluatorToAdd : plan.getEvaluatorsToAdd(NAMESPACE_DOLPHIN_BSP)) {
          LOG.log(Level.INFO, "Add new evaluator {0}", evaluatorToAdd);
          elasticMemory.add(1, evalSize, 1, evaluatorAllocatedHandler, contextActiveHandlers);
        }
        executingPlan.awaitActiveContexts();

        LOG.log(Level.INFO, "All evaluators were added, will transfer data.");
        // TODO #90: The try-catch is needed to close contexts on network failure in EM.move.
        // TODO #90: Need to revisit whether EM.move should throw a RuntimeException on network failure.
        // TODO #90: Perhaps the handlers should receive this failure information instead.
        try {
          for (final TransferStep transferStep : plan.getTransferSteps(NAMESPACE_DOLPHIN_BSP)) {
            elasticMemory.move(
                transferStep.getDataInfo().getNumBlocks(),
                transferStep.getSrcId(),
                executingPlan.getActualContextId(transferStep.getDstId()),
                new MovedHandler());
          }
        } catch (final Exception e) {
          LOG.log(Level.WARNING, "Caught Exception, closing Evaluators.", e);
          executingPlan.onSynchronizedExecutionFailed();
        }
        executingPlan.awaitMoves();

        LOG.log(Level.INFO, "All data transfers were completed, will submit tasks and delete evaluators on pause.");
        driverSync.execute(new SynchronizedExecutionHandler(executingPlan),
            new SynchronizedExecutionFailedHandler(executingPlan));
        executingPlan.awaitSynchronizedExecution();

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
    LOG.log(Level.FINE, "RunningTask {0}", task);
    executingPlan.onRunningTask(task);
  }

  @Override
  public void onCompletedTask(final CompletedTask task) {
    LOG.log(Level.FINE, "CompletedTask {0}", task);
  }

  /**
   * This handler is registered as a callback to ElasticMemory.add().
   */
  private final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      final Configuration contextConf = dolphinDriver.get().getContextConfiguration();
      final String contextId = getContextId(contextConf);
      final Configuration serviceConf = dolphinDriver.get().getServiceConfiguration(contextId);
      allocatedEvaluator.submitContextAndService(contextConf, serviceConf);
    }
  }

  /**
   * Return the ID of the given Context.
   */
  private String getContextId(final Configuration contextConf) {
    try {
      final Injector injector = Tang.Factory.getTang().newInjector(contextConf);
      return injector.getNamedInstance(ContextIdentifier.class);
    } catch (final InjectionException e) {
      throw new RuntimeException("Unable to get the context's identifier from the configuration", e);
    }
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
   * This handler is registered as the second callback to ElasticMemory.move().
   */
  private final class MovedHandler implements EventHandler<AvroElasticMemoryMessage> {
    @Override
    public void onNext(final AvroElasticMemoryMessage msg) {
      LOG.log(Level.INFO, "Received new MoveFinished {0}.", msg);
      if (msg.getResultMsg().getResult() == Result.FAILURE) {
        LOG.log(Level.WARNING, "Move failed because {0}", msg.getResultMsg().getMsg());
      }
      if (executingPlan == null) {
        throw new RuntimeException("MoveFinished " + msg + " received, but no executingPlan available.");
      }
      executingPlan.onMoved();
    }
  }

  /**
   * This handler is registered as the first callback to DriverSync.execute().
   *
   * Runs the following steps, before an iteration (while the Tasks are paused):
   * 1. Submit tasks
   * 2. Delete evaluators
   * 3. Wait for all tasks to start running and all deletions to complete
   * 4. Notify completed execution
   *
   */
  private final class SynchronizedExecutionHandler implements EventHandler<IterationInfo> {
    private final ExecutingPlan executingPlan;
    
    SynchronizedExecutionHandler(final ExecutingPlan executingPlan) {
      this.executingPlan = executingPlan;
    }
    
    @Override
    public void onNext(final IterationInfo iterationInfo) {
      LOG.log(Level.INFO, "Starting synchronized execution on iteration {0}.", iterationInfo);
      if (executingPlan == null) {
        throw new RuntimeException("Synchronized execution on " + iterationInfo + ", but no executingPlan available.");
      }

      LOG.log(Level.INFO, "moves finished, submitting tasks.");
      for (final ActiveContext context : executingPlan.getContextsToSubmit()) {
        dolphinDriver.get().submitTask(context, iterationInfo);
      }
      LOG.log(Level.INFO, "deleting evaluators.");
      for (final String evaluatorId : executingPlan.getEvaluatorsToDelete()) {
        elasticMemory.delete(evaluatorId, new DeletedHandler());
      }

      LOG.log(Level.INFO, "Waiting for RunningTasks.");
      final Collection<RunningTask> addedTasks = executingPlan.awaitRunningTasks();
      for (final RunningTask task : addedTasks) {
        LOG.log(Level.INFO, "Newly added task {0}", task);
      }
      LOG.log(Level.INFO, "Waiting for Deleted Evaluators.");
      executingPlan.awaitDeletes();

      executingPlan.onSynchronizedExecutionCompleted();
    }
  }

  /**
   * This handler is registered as the second callback to DriverSync.execute().
   *
   * Calls the relevant executingPlan methods to close any outstanding contexts.
   */
  private final class SynchronizedExecutionFailedHandler implements EventHandler<Object> {
    private final ExecutingPlan executingPlan;

    SynchronizedExecutionFailedHandler(final ExecutingPlan executingPlan) {
      this.executingPlan = executingPlan;
    }

    @Override
    public void onNext(final Object value) {
      executingPlan.onSynchronizedExecutionFailed();
    }
  }


  /**
   * This handler is registered as the callback to ElasticMemory.delete().
   */
  private final class DeletedHandler implements EventHandler<AvroElasticMemoryMessage> {
    @Override
    public void onNext(final AvroElasticMemoryMessage msg) {
      LOG.log(Level.INFO, "Received new Evaluators Deleted {0}", msg);

      final ResultMsg resultMsg = msg.getResultMsg();

      if (resultMsg.getResult() == Result.FAILURE) {
        LOG.log(Level.WARNING, "Evaluator delete failed for evaluator {0}", resultMsg.getSrcId());
      }
      if (executingPlan == null) {
        throw new RuntimeException("Evaluators deleted " + msg + " received, but no executingPlan available.");
      }
      executingPlan.onDeleted();
    }
  }

  /**
   * Encapsulates a single executing plan and its state.
   * By referencing the current executing plan Callback handlers are implemented as stateless.
   *
   * The executing plan runs through a sequence of barriers implemented as CountDownLatches:
   * 1. Wait until all Contexts have been allocated as ActiveContexts (activeContextLatch)
   * 2. Wait until all Moves are complete (moveLatch)
   * [Begin Synchronized Execution]
   * 3. Wait until all Task submissions have completed as RunningTasks (runningTaskLatch)
   * 4. Wait until all Evaluators have been deleted (deleteLatch)
   * [End Synchronized Execution]
   * 5. Wait until the Synchronized Execution completes (synchronizedExecutionLatch)
   */
  private static final class ExecutingPlan {
    private final ConcurrentMap<String, ActiveContext> pendingTaskSubmissions;
    private final List<String> addEvaluatorIds;
    private final List<ActiveContext> activeContexts;
    private final ConcurrentMap<String, ActiveContext> addEvaluatorIdsToContexts;
    private final ConcurrentMap<String, RunningTask> runningTasks;
    private final List<String> deleteEvaluatorsIds;
    private final CountDownLatch activeContextLatch;
    private final CountDownLatch moveLatch;
    private final CountDownLatch runningTaskLatch;
    private final CountDownLatch deleteLatch;
    private final CountDownLatch synchronizedExecutionLatch;

    private ExecutingPlan(final Plan plan) {
      this.pendingTaskSubmissions = new ConcurrentHashMap<>();
      this.addEvaluatorIds = new ArrayList<>(plan.getEvaluatorsToAdd(NAMESPACE_DOLPHIN_BSP));
      this.activeContexts = new ArrayList<>(plan.getEvaluatorsToAdd(NAMESPACE_DOLPHIN_BSP).size());
      this.addEvaluatorIdsToContexts = new ConcurrentHashMap<>();
      this.runningTasks = new ConcurrentHashMap<>();
      this.deleteEvaluatorsIds = new ArrayList<>(plan.getEvaluatorsToDelete(NAMESPACE_DOLPHIN_BSP));
      this.activeContextLatch = new CountDownLatch(plan.getEvaluatorsToAdd(NAMESPACE_DOLPHIN_BSP).size());
      this.moveLatch = new CountDownLatch(plan.getTransferSteps(NAMESPACE_DOLPHIN_BSP).size());
      this.runningTaskLatch = new CountDownLatch(plan.getEvaluatorsToAdd(NAMESPACE_DOLPHIN_BSP).size());
      this.deleteLatch = new CountDownLatch(plan.getEvaluatorsToDelete(NAMESPACE_DOLPHIN_BSP).size());
      this.synchronizedExecutionLatch = new CountDownLatch(1);
    }

    public void awaitActiveContexts() {
      try {
        activeContextLatch.await();
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      }
      activeContexts.addAll(pendingTaskSubmissions.values());

      for (int i = 0; i < activeContexts.size(); i++) {
        addEvaluatorIdsToContexts.put(addEvaluatorIds.get(i), activeContexts.get(i));
      }
    }

    public void onActiveContext(final ActiveContext context) {
      pendingTaskSubmissions.put(context.getId(), context);
      activeContextLatch.countDown();
    }

    /**
     * Translates a temporary context ID used in the plan to an addressable context ID.
     * This is done by keeping track of the IDs of newly created active contexts.
     *
     * If the param is recognized as a temporary ID then its newly created context's ID is returned.
     * If not, the context ID is already addressable, so it is returned as-is.
     *
     * @param planContextId context ID supplied by the plan
     * @return an addressable context ID
     */
    public String getActualContextId(final String planContextId) {
      return addEvaluatorIdsToContexts.containsKey(planContextId) ?
          addEvaluatorIdsToContexts.get(planContextId).getId() : planContextId;
    }

    public void awaitMoves() {
      try {
        moveLatch.await();
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    public void onMoved() {
      moveLatch.countDown();
    }

    public void onSynchronizedExecutionFailed() {
      LOG.log(Level.INFO, "Closing all outstanding contexts on cancel.");
      for (final ActiveContext context : activeContexts) {
        context.close();
      }
    }

    public Collection<ActiveContext> getContextsToSubmit() {
      return activeContexts;
    }

    public Collection<RunningTask> awaitRunningTasks() {
      try {
        runningTaskLatch.await();
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      }
      return runningTasks.values();
    }

    public void onRunningTask(final RunningTask task) {
      if (pendingTaskSubmissions.containsKey(task.getActiveContext().getId())) {
        runningTasks.put(task.getId(), task);
        runningTaskLatch.countDown();
      }
    }

    public Collection<String> getEvaluatorsToDelete() {
      return deleteEvaluatorsIds;
    }

    public void awaitDeletes() {
      try {
        deleteLatch.await();
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    public void onDeleted() {
      deleteLatch.countDown();
    }

    public void awaitSynchronizedExecution() {
      try {
        synchronizedExecutionLatch.await();
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    public void onSynchronizedExecutionCompleted() {
      synchronizedExecutionLatch.countDown();
    }
  }
}
