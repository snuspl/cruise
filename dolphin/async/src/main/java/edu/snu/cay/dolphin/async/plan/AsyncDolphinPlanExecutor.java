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
package edu.snu.cay.dolphin.async.plan;

import edu.snu.cay.dolphin.async.AsyncDolphinDriver;
import edu.snu.cay.dolphin.async.optimizer.ServerEM;
import edu.snu.cay.dolphin.async.optimizer.WorkerEM;
import edu.snu.cay.services.em.avro.EMMigrationMsg;
import edu.snu.cay.services.em.avro.Result;
import edu.snu.cay.services.em.avro.ResultMsg;
import edu.snu.cay.services.em.driver.api.ElasticMemory;
import edu.snu.cay.services.em.plan.api.*;
import edu.snu.cay.services.em.plan.impl.PlanResultImpl;
import edu.snu.cay.services.ps.driver.impl.EMRoutingTableManager;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.cay.dolphin.async.optimizer.parameters.Constants.NAMESPACE_WORKER;
import static edu.snu.cay.dolphin.async.optimizer.parameters.Constants.NAMESPACE_SERVER;
import static edu.snu.cay.dolphin.async.plan.DolphinPlanOperation.SYNC_OP;
import static edu.snu.cay.dolphin.async.plan.DolphinPlanOperation.START_OP;
import static edu.snu.cay.dolphin.async.plan.DolphinPlanOperation.STOP_OP;
import static edu.snu.cay.services.em.plan.impl.EMPlanOperation.ADD_OP;
import static edu.snu.cay.services.em.plan.impl.EMPlanOperation.DEL_OP;
import static edu.snu.cay.services.em.plan.impl.EMPlanOperation.MOVE_OP;

/**
 * Implementation of Plan Executor for AsyncDolphin.
 */
public final class AsyncDolphinPlanExecutor implements PlanExecutor {
  private static final Logger LOG = Logger.getLogger(AsyncDolphinPlanExecutor.class.getName());

  private static final int DEFAULT_EVAL_MEM_SIZE = 1024;
  private static final int DEFAULT_EVAL_NUM_CORES = 1;

  private enum OpExecutionStatus {
    IN_PROGRESS, COMPLETE
  }

  private final ElasticMemory serverEM;
  private final ElasticMemory workerEM;

  private final InjectionFuture<AsyncDolphinDriver> asyncDolphinDriver;

  private final EMRoutingTableManager routingTableManager;

  private final ExecutorService mainExecutor = Executors.newSingleThreadExecutor();

  /**
   * Object for representing the state of plan in execution.
   * Its lifecycle is same with the scope of {@link #execute(Plan)}.
   */
  private volatile ExecutingPlan executingPlan;

  /**
   * A counter to assign ids when allocating new Evaluators.
   */
  private AtomicInteger addedEvalCounter = new AtomicInteger(0);

  /**
   * Set of operations ready to be executed.
   */
  private final BlockingQueue<Set<PlanOperation>> nextOpsToExecuteInParallel = new LinkedBlockingQueue<>();

  @Inject
  private AsyncDolphinPlanExecutor(final InjectionFuture<AsyncDolphinDriver> asyncDolphinDriver,
                                   @Parameter(ServerEM.class) final ElasticMemory serverEM,
                                   @Parameter(WorkerEM.class) final ElasticMemory workerEM,
                                   final EMRoutingTableManager routingTableManager) {
    this.asyncDolphinDriver = asyncDolphinDriver;
    this.serverEM = serverEM;
    this.workerEM = workerEM;
    this.routingTableManager = routingTableManager;
  }

  /**
   * Executes a plan using ElasticMemory.
   *
   * The execution of a {@link Plan} is executed concurrently for independent EM operations.
   * Dependency is given by the DAG representation of a plan.
   * Once an event handler to add/move/delete operation is called back after the EM operation completes,
   * plan executor marks the operation as complete
   * and executes the next round of independent EM operations concurrently.
   *
   * @param plan to execute
   * @return execution result
   */
  @Override
  public synchronized Future<PlanResult> execute(final Plan plan) {
    LOG.log(Level.INFO, "Start executing the plan: {0}", plan);

    executingPlan = new ExecutingPlan(plan);

    final Set<PlanOperation> initialPlanOperations = executingPlan.getInitialOpsToExecute();
    LOG.log(Level.INFO, "Start with initial ops: {0}", initialPlanOperations);

    try {
      nextOpsToExecuteInParallel.put(initialPlanOperations);
    } catch (final InterruptedException e) {
      LOG.log(Level.WARNING, "Interrupted while putting ops into the queue", e);
    }

    final int numTotalOps = plan.getPlanSize();

    return mainExecutor.submit(new Callable<PlanResult>() {

      @Override
      public PlanResult call() throws Exception {

        int numStartedOps = 0;

        // check whether it starts all the operations in the plan
        while (numStartedOps < numTotalOps) {
          final Set<PlanOperation> nextOps = nextOpsToExecuteInParallel.take();

          if (nextOps != null) {
            executeOperations(nextOps);
            numStartedOps += nextOps.size();
          }
          LOG.log(Level.INFO, "numStartedOps: {0}, numTotalOps: {1}", new Object[]{numStartedOps, numTotalOps});
        }

        // wait until all the started operations are finished
        executingPlan.waitPlanExecution();

        final ConcurrentMap<PlanOperation, OpExecutionStatus> planExecutionResult =
                executingPlan.getPlanExecutionStatus();

        executingPlan = null;

        return new PlanResultImpl("Plan Execution Complete!\n[SUMMARY]\n" +
                planExecutionResult, planExecutionResult.size());
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

      final String workerId = "WORKER_ADDED_EVAL" + workerIndex;

      final Configuration idConfiguration = ContextConfiguration.CONF
          .set(ContextConfiguration.IDENTIFIER, workerId)
          .build();
      allocatedEvaluator.submitContext(idConfiguration);
    }
  }

  /**
   * This handler is registered as the active context callback of ElasticMemory.add().
   */
  private List<EventHandler<ActiveContext>> getActiveContextHandler(final String namespace,
                                                                    final PlanOperation addOperation) {
    final List<EventHandler<ActiveContext>> activeContextHandlers = new ArrayList<>(2);
    switch (namespace) {
    case NAMESPACE_SERVER:
      activeContextHandlers.add(asyncDolphinDriver.get().getFirstContextActiveHandlerForServer(true));
      activeContextHandlers.add(new ServerContextActiveHandler(addOperation));
      break;
    case NAMESPACE_WORKER:
      activeContextHandlers.add(asyncDolphinDriver.get().getFirstContextActiveHandlerForWorker(true));
      activeContextHandlers.add(new WorkerContextActiveHandler(addOperation));
      break;
    default:
      throw new RuntimeException("Unsupported namespace");
    }
    return activeContextHandlers;
  }

  /**
   * This handler is registered as a callback to ElasticMemory.add() for servers.
   */
  private final class ServerContextActiveHandler implements EventHandler<ActiveContext> {
    private final PlanOperation completedOp;

    private ServerContextActiveHandler(final PlanOperation completedOp) {
      this.completedOp = completedOp;
    }

    @Override
    public void onNext(final ActiveContext context) {
      if (executingPlan == null) {
        throw new RuntimeException("ActiveContext " + context + " received, but no executingPlan available.");
      }
      asyncDolphinDriver.get().getSecondContextActiveHandlerForServer().onNext(context);
      executingPlan.putAddedServerContext(completedOp.getEvalId().get(), context);
      onOperationComplete(completedOp);
    }
  }

  /**
   * This handler is registered as a callback to ElasticMemory.add() for workers.
   */
  private final class WorkerContextActiveHandler implements EventHandler<ActiveContext> {
    private final PlanOperation completedOp;

    private WorkerContextActiveHandler(final PlanOperation completedOp) {
      this.completedOp = completedOp;
    }
    @Override
    public void onNext(final ActiveContext context) {
      if (executingPlan == null) {
        throw new RuntimeException("ActiveContext " + context + " received, but no executingPlan available.");
      }

      executingPlan.putAddedWorkerContext(completedOp.getEvalId().get(), context);
      onOperationComplete(completedOp);
    }
  }

  /**
   * This handler is registered as the second callback to ElasticMemory.move().
   */
  private final class MovedHandler implements EventHandler<EMMigrationMsg> {
    private final PlanOperation completeOp;

    private MovedHandler(final PlanOperation completeOp) {
      this.completeOp = completeOp;
    }

    @Override
    public void onNext(final EMMigrationMsg msg) {
      LOG.log(Level.FINER, "Received new MoveFinished {0}.", msg);
      if (msg.getResultMsg().getResult() == Result.FAILURE) {
        LOG.log(Level.WARNING, "Move failed because {0}", msg.getResultMsg().getMsg());
      }
      if (executingPlan == null) {
        throw new RuntimeException("MoveFinished " + msg + " received, but no executingPlan available.");
      }
      onOperationComplete(completeOp);
    }
  }

  /**
   * This handler is registered as the callback to ElasticMemory.delete().
   */
  private final class DeletedHandler implements EventHandler<EMMigrationMsg> {
    private final PlanOperation completeOp;

    private DeletedHandler(final PlanOperation completeOp) {
      this.completeOp = completeOp;
    }

    @Override
    public void onNext(final EMMigrationMsg msg) {
      LOG.log(Level.FINER, "Received new Evaluators Deleted {0}", msg);

      final ResultMsg resultMsg = msg.getResultMsg();

      if (resultMsg.getResult() == Result.FAILURE) {
        LOG.log(Level.WARNING, "Evaluator delete failed for evaluator {0}", resultMsg.getSrcId());
      }
      if (executingPlan == null) {
        throw new RuntimeException("Evaluators deleted " + msg + " received, but no executingPlan available.");
      }
      onOperationComplete(completeOp);
    }
  }

  /**
   * This handler is registered as the callback to
   * {@link EMRoutingTableManager#checkWorkersTobeReadyForServerDelete(String, EventHandler)}.
   */
  private final class SyncHandler implements EventHandler<Void> {
    private final PlanOperation syncOp;

    private SyncHandler(final PlanOperation syncOp) {
      this.syncOp = syncOp;
    }

    @Override
    public void onNext(final Void aVoid) {
      LOG.log(Level.FINER, "Received Sync complete for server {0}", syncOp.getEvalId().get());
      if (executingPlan == null) {
        throw new RuntimeException("Sync operation is completed, but no executingPlan available.");
      }

      // we assume that Sync operation always succeeds
      onOperationComplete(syncOp);
    }
  }

  /**
   * Handles the result of Start operation.
   * It marks Start operation complete and execute the next operations, if they exist.
   */
  @Override
  public void onRunningTask(final RunningTask task) {
    LOG.log(Level.FINER, "RunningTask {0}", task);
    if (executingPlan == null) {
      return;
    }

    final Optional<PlanOperation> completedOp
        = executingPlan.removeWorkerTaskControlOp(task.getActiveContext().getId());
    if (completedOp.isPresent()) {
      onOperationComplete(completedOp.get());
    }
  }

  /**
   * Handles the result of Stop operation.
   * It marks Stop operation complete and execute the next operations, if they exist.
   */
  @Override
  public void onCompletedTask(final CompletedTask task) {
    LOG.log(Level.FINER, "CompletedTask {0}", task);
    if (executingPlan == null) {
      return;
    }

    final Optional<PlanOperation> completedOp
        = executingPlan.removeWorkerTaskControlOp(task.getActiveContext().getId());
    if (completedOp.isPresent()) {
      onOperationComplete(completedOp.get());
    }
  }

  /**
   * Refer to the steps explained in {@link Plan}.
   * TODO #764: need to consider failure in executing each operation
   */
  private void onOperationComplete(final PlanOperation completeOp) {
    final Set<PlanOperation> nextOpsToExecute = executingPlan.markOperationComplete(completeOp);
    LOG.log(Level.INFO, "CompleteOp: {0}, NextOps: {1}", new Object[]{completeOp, nextOpsToExecute});

    if (!nextOpsToExecute.isEmpty()) {
      try {
        nextOpsToExecuteInParallel.put(nextOpsToExecute);
      } catch (final InterruptedException e) {
        LOG.log(Level.WARNING, "Interrupted while putting ops into the queue", e);
      }
    }
  }

  /**
   * Executes the Plan operations by distinguishing each OpType as well as namespace.
   * If the operation is already IN_PROGRESS or COMPLETE,
   * the operation is skipped to prevent duplication.
   *
   * @param operationsToExecute a set of EM operations that can be executed independently at the point of trigger
   */
  private void executeOperations(final Set<PlanOperation> operationsToExecute) {
    try {
      for (final PlanOperation operation : operationsToExecute) {
        if (!executingPlan.markOperationRequested(operation)) {
          continue; // skip if it's already submitted
        }

        switch (operation.getOpType()) {
        case ADD_OP:
          executeAddOperation(operation);
          break;
        case DEL_OP:
          executeDelOperation(operation);
          break;
        case MOVE_OP:
          executeMoveOperation(operation);
          break;
        case SYNC_OP:
          executeSyncOperation(operation);
          break;
        case START_OP:
          executeStartOperation(operation);
          break;
        case STOP_OP:
          executeStopOperation(operation);
          break;
        default:
          throw new RuntimeException("Unsupported plan operation type");
        }
      }
    } catch (Exception e) {
      LOG.log(Level.WARNING, "Caught Exception, closing Evaluators.", e);
    }
  }

  private void executeAddOperation(final PlanOperation operation) {
    final String namespace = operation.getNamespace();

    switch (namespace) {
    case NAMESPACE_SERVER:
      LOG.log(Level.FINE, "ADD: server {0}", operation.getEvalId().get());
      serverEM.add(1, DEFAULT_EVAL_MEM_SIZE, DEFAULT_EVAL_NUM_CORES,
          getAllocatedEvalHandler(NAMESPACE_SERVER),
          getActiveContextHandler(NAMESPACE_SERVER, operation));
      break;
    case NAMESPACE_WORKER:
      LOG.log(Level.FINE, "ADD: worker {0}", operation.getEvalId().get());
      workerEM.add(1, DEFAULT_EVAL_MEM_SIZE, DEFAULT_EVAL_NUM_CORES,
          getAllocatedEvalHandler(NAMESPACE_WORKER),
          getActiveContextHandler(NAMESPACE_WORKER, operation));
      break;
    default:
      throw new RuntimeException("Unsupported namespace");
    }
  }

  private void executeDelOperation(final PlanOperation operation) {
    final String namespace = operation.getNamespace();
    final String evaluatorId = operation.getEvalId().get();

    switch (namespace) {
    case NAMESPACE_SERVER:
      LOG.log(Level.FINE, "DELETE: server {0}", evaluatorId);
      serverEM.delete(evaluatorId, new DeletedHandler(operation));
      break;
    case NAMESPACE_WORKER:
      LOG.log(Level.FINE, "DELETE: worker {0}", evaluatorId);
      workerEM.delete(evaluatorId, new DeletedHandler(operation));
      break;
    default:
      throw new RuntimeException("Unsupported namespace");
    }
  }

  private void executeMoveOperation(final PlanOperation operation) {
    final String namespace = operation.getNamespace();
    final TransferStep transferStep = operation.getTransferStep().get();

    final String destId;

    switch (namespace) {
    case NAMESPACE_SERVER:
      destId = executingPlan.getServerActualContextId(transferStep.getDstId());
      LOG.log(Level.FINE, "MOVE: server {0} -> {1}", new Object[]{transferStep.getSrcId(), destId});

      serverEM.move(
          transferStep.getDataInfo().getNumBlocks(),
          transferStep.getSrcId(),
          destId,
          new MovedHandler(operation));
      break;
    case NAMESPACE_WORKER:
      destId = executingPlan.getWorkerActualContextId(transferStep.getDstId());
      LOG.log(Level.FINE, "MOVE: worker {0} -> {1}", new Object[]{transferStep.getSrcId(), destId});

      workerEM.move(
          transferStep.getDataInfo().getNumBlocks(),
          transferStep.getSrcId(),
          destId,
          new MovedHandler(operation));
      break;
    default:
      throw new RuntimeException("Unsupported namespace");
    }
  }

  private void executeSyncOperation(final PlanOperation syncOp) {
    final String serverId = syncOp.getEvalId().get();
    LOG.log(Level.FINE, "SYNC: server {0}", serverId);

    routingTableManager.checkWorkersTobeReadyForServerDelete(serverId, new SyncHandler(syncOp));
  }

  private void executeStartOperation(final PlanOperation startOp) {
    final String planContextId = startOp.getEvalId().get();
    final Optional<ActiveContext> context = executingPlan.getAddedWorkerContext(planContextId);

    if (context.isPresent()) {
      final String contextId = context.get().getId();
      LOG.log(Level.FINE, "START: worker {0}", contextId);

      // submit task
      executingPlan.putWorkerTaskControlOp(contextId, startOp);
      asyncDolphinDriver.get().getSecondContextActiveHandlerForWorker(true).onNext(context.get());

    } else {
      throw new RuntimeException("There's no worker evaluator to start");
    }
  }

  private void executeStopOperation(final PlanOperation stopOp) {
    final String contextId = stopOp.getEvalId().get();

    // put metadata before trying to close worker task
    executingPlan.putWorkerTaskControlOp(contextId, stopOp);

    if (asyncDolphinDriver.get().closeWorkerTask(contextId)) {
      LOG.log(Level.INFO, "STOP: worker {0}", contextId);

    } else {
      executingPlan.removeWorkerTaskControlOp(stopOp.getEvalId().get());
      throw new RuntimeException("There's no worker evaluator to stop");
    }
  }

  /**
   * Encapsulates a single executing plan and its state in a DAG representation.
   * By referencing the current executing plan Callback handlers are implemented as stateless.
   *
   * The executing plan is initialized with a CountDownLatch value of the total number of operations.
   * For each complete operation, the handlers being called back count down the latch.
   * When all operations are complete, control is given back to the main plan executor's call().
   */
  private static final class ExecutingPlan {

    // In order to keep track of context mappings to evaluator IDs, we store the mappings when contexts are activated.
    private final ConcurrentMap<String, ActiveContext> planCtxIdToAddedServerCtx = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ActiveContext> planCtxIdToAddedWorkerCtx = new ConcurrentHashMap<>();

    /**
     * A bookkeeping of ongoing worker task Start/Stop operations.
     */
    private final ConcurrentMap<String, PlanOperation> ongoingWorkerTaskControlOps = new ConcurrentHashMap<>();

    /**
     * operationsRequested: A map of plan operations requested and the execution state (IN_PROGRESS/COMPLETE).
     */
    private final ConcurrentMap<PlanOperation, OpExecutionStatus> operationsRequested = new ConcurrentHashMap<>();

    /**
     * plan: An execution plan generated by optimizer, which represents dependencies between operations as a DAG.
     */
    private final Plan plan;

    /**
     * A countdown latch that releases waiting threads when the whole plan is finished.
     * It is counted down by {@link #markOperationComplete(PlanOperation)}.
     */
    private final CountDownLatch planExecutionLatch;

    private ExecutingPlan(final Plan plan) {
      this.plan = plan;
      this.planExecutionLatch = new CountDownLatch(plan.getPlanSize());
    }

    /**
     * Put added server context to keep track of them.
      @param planContextId a plan context id that is only valid in plan
     * @param context ActiveContext from the event handler
     */
    void putAddedServerContext(final String planContextId, final ActiveContext context) {
      planCtxIdToAddedServerCtx.put(planContextId, context);
    }

    /**
     * Put added worker context to keep track of them.
     * @param planContextId a plan context id that is only valid in plan
     * @param context ActiveContext from the event handler
     */
    void putAddedWorkerContext(final String planContextId, final ActiveContext context) {
      planCtxIdToAddedWorkerCtx.put(planContextId, context);
    }

    /**
     * Get added worker context whose plan id is {@code planContextId}.
     * @param planContextId a plan context id that is only valid on plan
     * @return an Optional with ActiveContext, it return an empty Optional when there's no matching ActiveContext
     */
    Optional<ActiveContext> getAddedWorkerContext(final String planContextId) {
      return Optional.ofNullable(planCtxIdToAddedWorkerCtx.get(planContextId));
    }

    /**
     * Put worker task control (START/STOP) operations.
     * @param contextId a context id
     * @param operation a Start or Stop operation
     */
    void putWorkerTaskControlOp(final String contextId, final PlanOperation operation) {
      ongoingWorkerTaskControlOps.put(contextId, operation);
      LOG.log(Level.INFO, "ongoingWorkerTaskControlOps: {0}, contextId: {1}",
          new Object[]{ongoingWorkerTaskControlOps, contextId});
    }

    /**
     * Remove and get worker task control (START/STOP) operations of {@code contextId}.
     * The {@code contextId} is the id of context on which the task is running.
     * @param contextId a context id
     * @return an optional with PlanOperation
     */
    Optional<PlanOperation> removeWorkerTaskControlOp(final String contextId) {
      LOG.log(Level.INFO, "ongoingWorkerTaskControlOps: {0}, contextId: {1}",
          new Object[]{ongoingWorkerTaskControlOps, contextId});
      return Optional.ofNullable(ongoingWorkerTaskControlOps.remove(contextId));
    }

    /**
     * Wait until the plan is completely executed.
     * @throws InterruptedException
     */
    void waitPlanExecution() throws InterruptedException {
      planExecutionLatch.await();
    }

    /**
     * Get a set of initial operations in the plan.
     * @return the set of operations
     */
    Set<PlanOperation> getInitialOpsToExecute() {
      return plan.getInitialOps();
    }

    /**
     * Puts the initiated plan operation into a map, mapping the operation to the execution status of IN_PROGRESS.
     *
     * @param operation that has just been initiated
     * @return true if operation has successfully been put into the <operation, status> map;
     *        false if the operation mapping already exists - i.e. is already in progress or complete.
     */
    boolean markOperationRequested(final PlanOperation operation) {
      LOG.log(Level.FINEST, "Operation requested: {0}", operation);
      return operationsRequested.putIfAbsent(operation, OpExecutionStatus.IN_PROGRESS) == null;
    }

    /**
     * Updates an operation's status as complete,
     * counts down the latch on the entire plan,
     * and gets the next set of operations that can be executed.
     *
     * @param operation the operation that has just been completed
     * @return the set of operations that can be executed after the operation's completion
     */
    Set<PlanOperation> markOperationComplete(final PlanOperation operation) {
      planExecutionLatch.countDown();
      final boolean wasInProgress = operationsRequested.replace(operation,
              OpExecutionStatus.IN_PROGRESS, OpExecutionStatus.COMPLETE);

      if (!wasInProgress) {
        throw new RuntimeException("The operation " + operation
            + " was never in the request queue or already complete");
      }

      return plan.onComplete(operation);
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
      return planCtxIdToAddedServerCtx.containsKey(planContextId) ?
          planCtxIdToAddedServerCtx.get(planContextId).getId() : planContextId;
    }

    String getWorkerActualContextId(final String planContextId) {
      return planCtxIdToAddedWorkerCtx.containsKey(planContextId) ?
          planCtxIdToAddedWorkerCtx.get(planContextId).getId() : planContextId;
    }

    /**
     * @return the map of operations already requested and each operation's status (IN_PROGRESS or COMPLETE)
     */
    ConcurrentMap<PlanOperation, OpExecutionStatus> getPlanExecutionStatus() {
      return operationsRequested;
    }
  }
}
