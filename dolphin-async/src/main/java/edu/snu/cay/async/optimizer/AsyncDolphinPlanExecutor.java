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

import edu.snu.cay.async.AsyncDolphinDriver;
import edu.snu.cay.services.em.avro.AvroElasticMemoryMessage;
import edu.snu.cay.services.em.avro.Result;
import edu.snu.cay.services.em.driver.api.ElasticMemory;
import edu.snu.cay.services.em.plan.api.Plan;
import edu.snu.cay.services.em.plan.api.PlanExecutor;
import edu.snu.cay.services.em.plan.api.PlanResult;
import edu.snu.cay.services.em.plan.api.TransferStep;
import edu.snu.cay.services.em.plan.impl.PlanResultImpl;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.cay.async.optimizer.OptimizationOrchestrator.NAMESPACE_SERVER;
import static edu.snu.cay.async.optimizer.OptimizationOrchestrator.NAMESPACE_WORKER;

/**
 * Implementation of Plan Executor for AsyncDolphin.
 */
public final class AsyncDolphinPlanExecutor implements PlanExecutor {
  private static final Logger LOG = Logger.getLogger(AsyncDolphinPlanExecutor.class.getName());

  private final ElasticMemory serverEM;
  private final ElasticMemory workerEM;

  private final InjectionFuture<AsyncDolphinDriver> asyncDolphinDriver;

  private final ExecutorService mainExecutor = Executors.newSingleThreadExecutor();

  private ExecutingPlan executingPlan;

  private AtomicInteger addedEvalCounter = new AtomicInteger(0);

  @Inject
  private AsyncDolphinPlanExecutor(final InjectionFuture<AsyncDolphinDriver> asyncDolphinDriver,
                                   @Parameter(ServerEM.class) final ElasticMemory serverEM,
                                   @Parameter(WorkerEM.class) final ElasticMemory workerEM) {
    this.asyncDolphinDriver = asyncDolphinDriver;
    this.serverEM = serverEM;
    this.workerEM = workerEM;
  }

  /**
   * Executes a plan using ElasticMemory and DriverSync.
   *
   * The main steps are as follows. Intermediate steps take place within respective handlers,
   * as summarized in {@link ExecutingPlan}.
   * 1. Create and assign an executingPlan
   * 2. Call ElasticMemory.add(), wait for active contexts
   * 3. Call ElasticMemory.move(), wait for data transfers to complete
   * 4. Call ElasticMemory.delete(), wait for closed contexts
   * 5. Clear executingPlan and return
   *
   * @param plan to execute
   * @return execution result
   */
  @Override
  public Future<PlanResult> execute(final Plan plan) {
    return mainExecutor.submit(new Callable<PlanResult>() {
      @Override
      public PlanResult call() throws Exception {
        final Collection<String> serverEvalsToAdd = plan.getEvaluatorsToAdd(NAMESPACE_SERVER);
        final Collection<String> serverEvalsToDel = plan.getEvaluatorsToDelete(NAMESPACE_SERVER);
        final Collection<TransferStep> serverTransferSteps = plan.getTransferSteps(NAMESPACE_SERVER);

        final Collection<String> workerEvalsToAdd = plan.getEvaluatorsToAdd(NAMESPACE_WORKER);
        final Collection<String> workerEvalsToDel = plan.getEvaluatorsToDelete(NAMESPACE_WORKER);
        final Collection<TransferStep> workerTransferSteps = plan.getTransferSteps(NAMESPACE_WORKER);

        if (serverEvalsToAdd.isEmpty() && serverEvalsToDel.isEmpty() && serverTransferSteps.isEmpty() &&
            workerEvalsToAdd.isEmpty() && workerEvalsToDel.isEmpty() && workerTransferSteps.isEmpty()) {
          return new PlanResultImpl();
        }

        executingPlan = new ExecutingPlan(plan);

        for (final String serverEval : serverEvalsToAdd) {
          // TODO #00: Make the evaluator size configurable in EM.add().
          serverEM.add(1, 1024, 1,
              getAllocatedEvalHandler(NAMESPACE_SERVER),
              getActiveContextHandler(NAMESPACE_SERVER));
        }

        for (final String workerEval : workerEvalsToAdd) {
          // TODO #00: Make the evaluator size configurable in EM.add().
          workerEM.add(1, 1024, 1,
              getAllocatedEvalHandler(NAMESPACE_WORKER),
              getActiveContextHandler(NAMESPACE_WORKER));
        }

        executingPlan.awaitActiveContexts();

        LOG.log(Level.INFO, "All evaluators were added, will transfer data.");
        // TODO #90: The try-catch is needed to close contexts on network failure in EM.move.
        // TODO #90: Need to revisit whether EM.move should throw a RuntimeException on network failure.
        // TODO #90: Perhaps the handlers should receive this failure information instead.
        try {
          for (final TransferStep transferStep : plan.getTransferSteps(NAMESPACE_SERVER)) {
            serverEM.move(
                transferStep.getDataInfo().getDataType(),
                transferStep.getDataInfo().getNumUnits(), // NumUnits is treated as block number.
                transferStep.getSrcId(),
                executingPlan.getServerActualContextId(transferStep.getDstId()),
                new MovedHandler());
          }
          for (final TransferStep transferStep : plan.getTransferSteps(NAMESPACE_WORKER)) {
            workerEM.move(
                transferStep.getDataInfo().getDataType(),
                transferStep.getDataInfo().getNumUnits(),
                transferStep.getSrcId(),
                executingPlan.getWorkerActualContextId(transferStep.getDstId()),
                new MovedHandler());
          }
        } catch (final Exception e) {
          LOG.log(Level.WARNING, "Caught Exception, closing Evaluators.", e);
        }

        executingPlan.awaitMoves();

        // 3. execute delete plans
        LOG.log(Level.INFO, "All data transfers were completed, will delete evaluators.");
        for (final String evaluatorId : executingPlan.getServerEvaluatorsToDelete()) {
          serverEM.delete(evaluatorId, new DeletedHandler());
        }
        for (final String evaluatorId : executingPlan.getWorkerEvaluatorsToDelete()) {
          workerEM.delete(evaluatorId, new DeletedHandler());
        }
        executingPlan.awaitDeletes();


        return new PlanResultImpl();
      }
    });
  }

  /**
   * This handler is registered as the allocated evaluator callback of ElasticMemory.add().
   */
  private EventHandler<AllocatedEvaluator> getAllocatedEvalHandler(final String namespace) {
    final EventHandler<AllocatedEvaluator> eventHandler;
    switch (namespace) {
    case NAMESPACE_SERVER:
      eventHandler = asyncDolphinDriver.get().getEvalAllocHandlerForServer();
      break;
    case NAMESPACE_WORKER:
      eventHandler = new WorkerEvaluatorAllocatedHandler();
      break;
    default:
      throw new RuntimeException("Unsupported namespace");
    }
    return eventHandler;
  }

  /**
   * This handler is registered as a callback to ElasticMemory.add() for Workers.
   */
  private final class WorkerEvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      LOG.log(Level.FINE, "Submitting Compute Context to {0}", allocatedEvaluator.getId());
      final int workerIndex = addedEvalCounter.getAndIncrement();
      final Configuration idConfiguration = ContextConfiguration.CONF
          .set(ContextConfiguration.IDENTIFIER, "WORKER_ADDED_EVAL" + workerIndex)
          .build();
      allocatedEvaluator.submitContext(idConfiguration);
    }
  }

    /**
   * This handler is registered as the active context callback of ElasticMemory.add().
   */
  private List<EventHandler<ActiveContext>> getActiveContextHandler(final String namespace) {
    final List<EventHandler<ActiveContext>> activeContextHandlers = new ArrayList<>(2);
    switch (namespace) {
    case NAMESPACE_SERVER:
      activeContextHandlers.add(asyncDolphinDriver.get().getFirstContextActiveHandlerForServer());
      activeContextHandlers.add(asyncDolphinDriver.get().getSecondContextActiveHandlerForServer());
      break;
    case NAMESPACE_WORKER:
      activeContextHandlers.add(asyncDolphinDriver.get().getFirstContextActiveHandlerForWorker());
      activeContextHandlers.add(asyncDolphinDriver.get().getSecondContextActiveHandlerForWorker());
      activeContextHandlers.add(new ContextActiveHandler());
      break;
    default:
      throw new RuntimeException("Unsupported namespace");
    }
    return activeContextHandlers;
  }

  @Override
  public void onRunningTask(final RunningTask task) {
    if (executingPlan == null) {
      return;
    }
//    executingPlan.onRunningTask(task);
    LOG.info("onRunningTask!");
  }

  /**
   * This handler is registered as a callback to ElasticMemory.add().
   */
  private final class ContextActiveHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext context) {
      if (executingPlan == null) {
        throw new RuntimeException("ActiveContext " + context + " received, but no executingPlan available.");
      }

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
   * This handler is registered as the callback to ElasticMemory.delete().
   */
  private final class DeletedHandler implements EventHandler<AvroElasticMemoryMessage> {
    @Override
    public void onNext(final AvroElasticMemoryMessage msg) {
      LOG.log(Level.INFO, "Received new Evaluators Deleted {0}", msg);
      if (msg.getResultMsg().getResult() == Result.FAILURE) {
        LOG.log(Level.WARNING, "Evaluator delete failed for evaluator {0}", msg.getSrcId());
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
   * 2-1. Wait until all Task submissions have completed as RunningTasks (runningTaskLatch)
   * 3. Wait until all Evaluators have been deleted (deleteLatch)
   */
  private static final class ExecutingPlan {
    private final List<String> addServerEvaluatorIds;
    private final List<ActiveContext> serverActiveContexts;
    private final ConcurrentMap<String, ActiveContext> addServerEvaluatorIdsToContexts;

    private final List<String> addWorkerEvaluatorIds;
    private final List<ActiveContext> workerActiveContexts;
    private final ConcurrentMap<String, ActiveContext> addWorkerEvaluatorIdsToContexts;

    private final List<String> deleteServerEvaluatorsIds;
    private final List<String> deleteWorkerEvaluatorsIds;

    private final CountDownLatch activeContextLatch;
    private final CountDownLatch moveLatch;
    private final CountDownLatch deleteLatch;

    private ExecutingPlan(final Plan plan) {
      this.addServerEvaluatorIds = new ArrayList<>(plan.getEvaluatorsToAdd(NAMESPACE_SERVER));
      this.serverActiveContexts = new ArrayList<>(plan.getEvaluatorsToAdd(NAMESPACE_SERVER).size());
      this.addServerEvaluatorIdsToContexts = new ConcurrentHashMap<>();
      this.deleteServerEvaluatorsIds = new ArrayList<>(plan.getEvaluatorsToDelete(NAMESPACE_SERVER));

      this.addWorkerEvaluatorIds = new ArrayList<>(plan.getEvaluatorsToAdd(NAMESPACE_WORKER));
      this.workerActiveContexts = new ArrayList<>(plan.getEvaluatorsToAdd(NAMESPACE_WORKER).size());
      this.addWorkerEvaluatorIdsToContexts = new ConcurrentHashMap<>();
      this.deleteWorkerEvaluatorsIds = new ArrayList<>(plan.getEvaluatorsToDelete(NAMESPACE_WORKER));

      this.activeContextLatch = new CountDownLatch(addServerEvaluatorIds.size() + addWorkerEvaluatorIds.size());
      this.moveLatch = new CountDownLatch(
          plan.getTransferSteps(NAMESPACE_SERVER).size() +
          plan.getTransferSteps(NAMESPACE_WORKER).size());

      this.deleteLatch = new CountDownLatch(
          plan.getEvaluatorsToDelete(NAMESPACE_SERVER).size() +
          plan.getEvaluatorsToDelete(NAMESPACE_WORKER).size());
    }

    void awaitActiveContexts() {
      try {
        activeContextLatch.await();
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      }
      LOG.info("All ActiveContexts are arrived!");

      for (int i = 0; i < workerActiveContexts.size(); i++) {
        addWorkerEvaluatorIdsToContexts.put(addWorkerEvaluatorIds.get(i), workerActiveContexts.get(i));
      }
      for (int i = 0; i < serverActiveContexts.size(); i++) {
        addServerEvaluatorIdsToContexts.put(addServerEvaluatorIds.get(i), serverActiveContexts.get(i));
      }
    }

    void onActiveContext(final ActiveContext context) {
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
    String getServerActualContextId(final String planContextId) {
      return addServerEvaluatorIdsToContexts.containsKey(planContextId) ?
          addServerEvaluatorIdsToContexts.get(planContextId).getId() : planContextId;
    }

    String getWorkerActualContextId(final String planContextId) {
      return addWorkerEvaluatorIdsToContexts.containsKey(planContextId) ?
          addWorkerEvaluatorIdsToContexts.get(planContextId).getId() : planContextId;
    }

    void awaitMoves() {
      try {
        moveLatch.await();
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    void onMoved() {
      moveLatch.countDown();
    }

    Collection<String> getServerEvaluatorsToDelete() {
      return deleteServerEvaluatorsIds;
    }

    Collection<String> getWorkerEvaluatorsToDelete() {
      return deleteWorkerEvaluatorsIds;
    }

    void awaitDeletes() {
      try {
        deleteLatch.await();
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    void onDeleted() {
      deleteLatch.countDown();
    }
  }
}
