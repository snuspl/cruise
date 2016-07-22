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

import edu.snu.cay.dolphin.async.AsyncDolphinDriver;
import edu.snu.cay.services.em.avro.AvroElasticMemoryMessage;
import edu.snu.cay.services.em.avro.Result;
import edu.snu.cay.services.em.driver.api.ElasticMemory;
import edu.snu.cay.services.em.plan.api.Plan;
import edu.snu.cay.services.em.plan.api.PlanExecutor;
import edu.snu.cay.services.em.plan.api.PlanResult;
import edu.snu.cay.services.em.plan.api.TransferStep;
import edu.snu.cay.services.em.plan.impl.EMOperation;
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
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.cay.dolphin.async.optimizer.OptimizationOrchestrator.NAMESPACE_SERVER;
import static edu.snu.cay.dolphin.async.optimizer.OptimizationOrchestrator.NAMESPACE_WORKER;

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
  private final BlockingQueue<Set<EMOperation>> nextOpsToExecuteInParallel = new LinkedBlockingQueue<>();

  @Inject
  private AsyncDolphinPlanExecutor(final InjectionFuture<AsyncDolphinDriver> asyncDolphinDriver,
                                   @Parameter(ServerEM.class) final ElasticMemory serverEM,
                                   @Parameter(WorkerEM.class) final ElasticMemory workerEM) {
    this.asyncDolphinDriver = asyncDolphinDriver;
    this.serverEM = serverEM;
    this.workerEM = workerEM;
    serverEM.addGroup(NAMESPACE_SERVER);
    workerEM.addGroup(NAMESPACE_WORKER);
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
    executingPlan = new ExecutingPlan(plan);

    final Set<EMOperation> initialEMOperations = executingPlan.getNextOpsToExecute();
    try {
      nextOpsToExecuteInParallel.put(initialEMOperations);
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
          final Set<EMOperation> nextOps = nextOpsToExecuteInParallel.take();

          if (nextOps != null) {
            executeOperations(nextOps);
            numStartedOps += nextOps.size();
          }
        }

        // wait until all the started operations are finished
        executingPlan.waitPlanExecution();

        final ConcurrentMap<EMOperation, OpExecutionStatus> planExecutionResult =
                executingPlan.getPlanExecutionStatus();

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

      // Note that this id is temporary and used by PlanExecutor internally.
      // The actual Evaluator id is assigned by REEF, when it is allocated.
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
                                                                    final EMOperation operation) {
    final List<EventHandler<ActiveContext>> activeContextHandlers = new ArrayList<>(2);
    switch (namespace) {
    case NAMESPACE_SERVER:
      activeContextHandlers.add(asyncDolphinDriver.get().getFirstContextActiveHandlerForServer(true));
      activeContextHandlers.add(new ServerContextActiveHandler(operation));
      break;
    case NAMESPACE_WORKER:
      activeContextHandlers.add(asyncDolphinDriver.get().getFirstContextActiveHandlerForWorker(true));
      activeContextHandlers.add(new WorkerContextActiveHandler(operation));
      break;
    default:
      throw new RuntimeException("Unsupported namespace");
    }
    return activeContextHandlers;
  }

  @Override
  public void onRunningTask(final RunningTask task) {
    LOG.log(Level.FINE, "RunningTask {0}", task);
  }

  /**
   * This handler is registered as a callback to ElasticMemory.add() for servers.
   */
  private final class ServerContextActiveHandler implements EventHandler<ActiveContext> {
    private final EMOperation completedOp;

    private ServerContextActiveHandler(final EMOperation completedOp) {
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
    private final EMOperation completedOp;

    private WorkerContextActiveHandler(final EMOperation completedOp) {
      this.completedOp = completedOp;
    }
    @Override
    public void onNext(final ActiveContext context) {
      if (executingPlan == null) {
        throw new RuntimeException("ActiveContext " + context + " received, but no executingPlan available.");
      }
      asyncDolphinDriver.get().getSecondContextActiveHandlerForWorker().onNext(context);
      executingPlan.putAddedWorkerContext(completedOp.getEvalId().get(), context);
      onOperationComplete(completedOp);
    }
  }

  /**
   * This handler is registered as the second callback to ElasticMemory.move().
   */
  private final class MovedHandler implements EventHandler<AvroElasticMemoryMessage> {
    private final EMOperation completeOp;

    MovedHandler(final EMOperation completeOp) {
      this.completeOp = completeOp;
    }

    @Override
    public void onNext(final AvroElasticMemoryMessage msg) {
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
  private final class DeletedHandler implements EventHandler<AvroElasticMemoryMessage> {
    private final EMOperation completeOp;

    DeletedHandler(final EMOperation completeOp) {
      this.completeOp = completeOp;
    }

    @Override
    public void onNext(final AvroElasticMemoryMessage msg) {
      LOG.log(Level.FINER, "Received new Evaluators Deleted {0}", msg);
      if (msg.getResultMsg().getResult() == Result.FAILURE) {
        LOG.log(Level.WARNING, "Evaluator delete failed for evaluator {0}", msg.getSrcId());
      }
      if (executingPlan == null) {
        throw new RuntimeException("Evaluators deleted " + msg + " received, but no executingPlan available.");
      }
      onOperationComplete(completeOp);
    }
  }

  /**
   * Refer to the steps explained in {@link Plan}.
   */
  private void onOperationComplete(final EMOperation completeOp) {
    final Set<EMOperation> nextOpsToExecute = executingPlan.markOperationComplete(completeOp);
    LOG.log(Level.FINEST, "Operation marked complete: {0}", completeOp);

    if (!nextOpsToExecute.isEmpty()) {
      LOG.log(Level.INFO, "Executing the next set of independent operations. " +
          "CompleteOp: {0}", completeOp);

      try {
        nextOpsToExecuteInParallel.put(nextOpsToExecute);
      } catch (final InterruptedException e) {
        LOG.log(Level.WARNING, "Interrupted while putting ops into the queue", e);
      }
    }
  }

  /**
   * Executes the EM operations by distinguishing each OpType as well as namespace.
   * If the operation is already in_progress or complete (operationStatus != null),
   * the operation is skipped to prevent duplication.
   *
   * @param operationsToExecute a set of EM operations that can be executed independently at the point of trigger
   */
  private void executeOperations(final Set<EMOperation> operationsToExecute) {
    try {
      for (final EMOperation operation : operationsToExecute) {
        final EMOperation.OpType opType = operation.getOpType();

        if (!executingPlan.markOperationRequested(operation)) {
          continue;
        }

        switch (opType) {
        case ADD:
          executeAddOperation(operation);
          break;
        case DEL:
          executeDelOperation(operation);
          break;
        case MOVE:
          executeMoveOperation(operation);
          break;
        default:
          throw new RuntimeException("Unsupported EM operation type");
        }
      }
    } catch (Exception e) {
      LOG.log(Level.WARNING, "Caught Exception, closing Evaluators.", e);
    }
  }

  private void executeAddOperation(final EMOperation operation) {
    final String namespace = operation.getNamespace();

    switch (namespace) {
    case NAMESPACE_SERVER:
      LOG.log(Level.FINE, "Adding server {0}", operation.getEvalId());
      serverEM.add(NAMESPACE_SERVER, 1, DEFAULT_EVAL_MEM_SIZE, DEFAULT_EVAL_NUM_CORES,
          getAllocatedEvalHandler(NAMESPACE_SERVER),
          getActiveContextHandler(NAMESPACE_SERVER, operation));
      break;
    case NAMESPACE_WORKER:
      LOG.log(Level.FINE, "Adding worker {0}", operation.getEvalId());
      workerEM.add(NAMESPACE_WORKER, 1, DEFAULT_EVAL_MEM_SIZE, DEFAULT_EVAL_NUM_CORES,
          getAllocatedEvalHandler(NAMESPACE_WORKER),
          getActiveContextHandler(NAMESPACE_WORKER, operation));
      break;
    default:
      throw new RuntimeException("Unsupported namespace");
    }
  }

  private void executeDelOperation(final EMOperation operation) {
    final String namespace = operation.getNamespace();
    final String evaluatorId = operation.getEvalId().get();

    switch (namespace) {
    case NAMESPACE_SERVER:
      LOG.log(Level.FINE, "Deleting server {0}", evaluatorId);
      serverEM.delete(evaluatorId, new DeletedHandler(operation));
      break;
    case NAMESPACE_WORKER:
      LOG.log(Level.FINE, "Deleting worker {0}", evaluatorId);
      workerEM.delete(evaluatorId, new DeletedHandler(operation));
      break;
    default:
      throw new RuntimeException("Unsupported namespace");
    }
  }

  private void executeMoveOperation(final EMOperation operation) {
    final String namespace = operation.getNamespace();
    final TransferStep transferStep = operation.getTransferStep().get();

    switch (namespace) {
    case NAMESPACE_SERVER:
      serverEM.move(
          transferStep.getDataInfo().getNumBlocks(),
          transferStep.getSrcId(),
          executingPlan.getServerActualContextId(transferStep.getDstId()),
          new MovedHandler(operation));
      break;
    case NAMESPACE_WORKER:
      workerEM.move(
          transferStep.getDataInfo().getNumBlocks(),
          transferStep.getSrcId(),
          executingPlan.getWorkerActualContextId(transferStep.getDstId()),
          new MovedHandler(operation));
      break;
    default:
      throw new RuntimeException("Unsupported namespace");
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
    private final ConcurrentMap<String, ActiveContext> ctxIdToAddedServerCtx;
    private final ConcurrentMap<String, ActiveContext> ctxIdToAddedWorkerCtx;

    /**
     * emOperationsRequested: A map of EM operations requested and the execution state (in_progress/complete).
     */
    private final ConcurrentMap<EMOperation, OpExecutionStatus> emOperationsRequested;

    /**
     * plan: An execution plan generated by optimizer, which represents dependencies between operations as a DAG.
     */
    private final Plan plan;

    /**
     * A countdown latch that releases waiting threads when the whole plan is finished.
     * It is counted down by {@link #markOperationComplete(EMOperation)}.
     */
    private final CountDownLatch planExecutionLatch;

    private ExecutingPlan(final Plan plan) {
      this.ctxIdToAddedServerCtx = new ConcurrentHashMap<>();
      this.ctxIdToAddedWorkerCtx = new ConcurrentHashMap<>();
      this.emOperationsRequested = new ConcurrentHashMap<>();
      this.plan = plan;
      this.planExecutionLatch = new CountDownLatch(plan.getPlanSize());
    }

    /**
     * Put added server context to keep track of them.
     * @param context ActiveContext from the event handler
     */
    void putAddedServerContext(final String planContextId, final ActiveContext context) {
      ctxIdToAddedServerCtx.put(planContextId, context);
    }

    /**
     * Put added worker context to keep track of them.
     * @param context ActiveContext from the event handler
     */
    void putAddedWorkerContext(final String planContextId, final ActiveContext context) {
      ctxIdToAddedWorkerCtx.put(planContextId, context);
    }

    /**
     * Wait until the plan is completely executed.
     * @throws InterruptedException
     */
    void waitPlanExecution() throws InterruptedException {
      planExecutionLatch.await();
    }

    /**
     * Get a set of operations in the plan that can be executed next.
     * @return the set of operations
     */
    Set<EMOperation> getNextOpsToExecute() {
      return plan.getReadyOps();
    }

    /**
     * Puts the initiated EM operation into a map, mapping the operation to the execution status of "In progress".
     *
     * @param operation that has just been initiated
     * @return true if operation has successfully been put into the <operation, status> map;
     *        false if the operation mapping already exists - i.e. is in progress or already complete.
     */
    boolean markOperationRequested(final EMOperation operation) {
      LOG.log(Level.FINEST, "Operation requested: {0}", operation);
      return emOperationsRequested.putIfAbsent(operation, OpExecutionStatus.IN_PROGRESS) == null;
    }

    /**
     * Updates an operation's status as complete,
     * counts down the latch on the entire plan,
     * gets the next set of operations that can be executed.
     *
     * @param op the operation that has just been completed
     * @return the set of operations that can be executed after op's completion
     */
    Set<EMOperation> markOperationComplete(final EMOperation op) {
      planExecutionLatch.countDown();
      final boolean wasInProgress = emOperationsRequested.replace(op,
              OpExecutionStatus.IN_PROGRESS, OpExecutionStatus.COMPLETE);

      if (!wasInProgress) {
        throw new RuntimeException("The operation " + op + " was never in the request queue");
      }

      return plan.onComplete(op);
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
      return ctxIdToAddedServerCtx.containsKey(planContextId) ?
          ctxIdToAddedServerCtx.get(planContextId).getId() : planContextId;
    }

    String getWorkerActualContextId(final String planContextId) {
      return ctxIdToAddedWorkerCtx.containsKey(planContextId) ?
          ctxIdToAddedWorkerCtx.get(planContextId).getId() : planContextId;
    }

    /**
     * @return the map of operations already requested and each operation's status (in progress or complete)
     */
    ConcurrentMap<EMOperation, OpExecutionStatus> getPlanExecutionStatus() {
      return emOperationsRequested;
    }
  }
}
