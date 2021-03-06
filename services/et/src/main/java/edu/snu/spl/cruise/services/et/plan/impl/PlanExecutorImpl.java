/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.spl.cruise.services.et.plan.impl;

import edu.snu.spl.cruise.services.et.common.util.concurrent.ListenableFuture;
import edu.snu.spl.cruise.services.et.common.util.concurrent.CompletedFuture;
import edu.snu.spl.cruise.services.et.common.util.concurrent.ResultFuture;
import edu.snu.spl.cruise.services.et.driver.api.ETMaster;
import edu.snu.spl.cruise.services.et.exceptions.PlanAlreadyExecutingException;
import edu.snu.spl.cruise.services.et.exceptions.PlanOpExecutionException;
import edu.snu.spl.cruise.services.et.metric.MetricManager;
import edu.snu.spl.cruise.services.et.plan.api.Op;
import edu.snu.spl.cruise.services.et.plan.api.PlanExecutor;
import edu.snu.spl.cruise.utils.CatchableExecutors;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An implementation of {@link PlanExecutor}.
 */
public final class PlanExecutorImpl implements PlanExecutor {
  private static final Logger LOG = Logger.getLogger(PlanExecutorImpl.class.getName());
  private static final int NUM_EXECUTOR_THREADS = 16;

  private final ETMaster etMaster;
  private final MetricManager metricManager;

  /**
   * A collection of listeners for the completion of operations.
   */
  private final Map<String, EventHandler<OpResult>> callbacks =
      Collections.synchronizedMap(new HashMap<String, EventHandler<OpResult>>());

  /**
   * A queue for sets of operations ready to be executed.
   * Each set is composed of operations that have no dependency to each other.
   */
  private final BlockingQueue<Set<Op>> nextOpsToExecuteInParallel = new LinkedBlockingQueue<>();

  /**
   * An abstraction to track the current progress of plan execution.
   * It's null, when there's no ongoing plan.
   */
  private final AtomicReference<ExecutingPlan> executingPlan = new AtomicReference<>();

  /**
   * A thread pool for parallelizing plan operations.
   * Actually it's not for execute the operation from end to end.
   * This thread pool parallelizes the part between the entry of {@link Op#execute} and its return of {@link Future}.
   */
  private final ExecutorService executor = CatchableExecutors.newFixedThreadPool(NUM_EXECUTOR_THREADS);

  @Inject
  private PlanExecutorImpl(final ETMaster etMaster,
                           final MetricManager metricManager) {
    this.etMaster = etMaster;
    this.metricManager = metricManager;
  }

  @Override
  public ListenableFuture<Void> execute(final ETPlan plan) throws PlanAlreadyExecutingException {
    if (executingPlan.get() != null) {
      throw new PlanAlreadyExecutingException("Executing more than one plan simultaneously is not allowed.");
    }

    if (plan.getNumTotalOps() == 0) {
      return new CompletedFuture<>(null);
    }

    // TODO #97: need to validate plan

    final long planStartTime = System.currentTimeMillis();

    executingPlan.set(new ExecutingPlan(plan));
    final Set<Op> ops = plan.getInitialOps();

    while (true) {
      try {
        nextOpsToExecuteInParallel.put(ops);
        break;
      } catch (InterruptedException e) {
        // ignore and keep waiting
      }
    }

    LOG.log(Level.INFO, "Start executing plan: {0}", plan);

    final ResultFuture<Void> resultFuture = new ResultFuture<>();
    CatchableExecutors.newSingleThreadExecutor().submit(() -> {
      // a map for virtual ids of new executors within this plan
      final Map<String, String> virtualIdToActualId = new ConcurrentHashMap<>();

      final int numTotalOps = plan.getNumTotalOps();
      int numStartedOps = 0;

      LOG.log(Level.INFO, "Num total ops: {0}", numTotalOps);
      // loop until it launches all ops
      while (numStartedOps < numTotalOps) {
        Set<Op> nextOps;

        while (true) {
          try {
            nextOps = nextOpsToExecuteInParallel.take();
            break;
          } catch (InterruptedException e) {
            // ignore and keep waiting
          }
        }

        if (nextOps != null) {
          // executes operations in parallel, because they have no dependency between them
          nextOps.forEach(op ->
              executor.submit(() -> {
                try {
                  LOG.log(Level.INFO, "Start executing op: {0}", op);
                  final long opStartTime = System.currentTimeMillis();
                  op.execute(etMaster, metricManager, virtualIdToActualId)
                      .addListener(opResult -> {
                        LOG.log(Level.INFO, "Op elapsed time (ms): {0}, OpId: {1}, OpType: {2}",
                            new Object[]{System.currentTimeMillis() - opStartTime, op.getOpId(), op.getOpType()});
                        onOpComplete(op, opResult);
                      });
                } catch (PlanOpExecutionException e) {
                  throw new RuntimeException(e);
                }
              }));

          numStartedOps += nextOps.size();
        }
        LOG.log(Level.INFO, "The number of started ops: [{0} / {1}]", new Object[]{numStartedOps, numTotalOps});
      }

      executingPlan.get().waitPlanCompletion();
      LOG.log(Level.INFO, "Plan elapsed time (ms): {0}", System.currentTimeMillis() - planStartTime);
      resultFuture.onCompleted(null);
      executingPlan.set(null);
    });

    return resultFuture;
  }

  @Override
  public void registerListener(final String id, final EventHandler<OpResult> callback) {
    callbacks.put(id, callback);
  }

  @Override
  public void unregisterListener(final String id) {
    callbacks.remove(id);
  }

  /**
   * Mark the operation as complete and make next operations ready.
   * @param op a completed operation
   */
  private synchronized void onOpComplete(final Op op, final OpResult opResult) {
    final ExecutingPlan ongoingPlan = executingPlan.get();
    if (ongoingPlan == null) {
      throw new RuntimeException("There's no ongoing plan");
    }

    callbacks.values().forEach(callback -> callback.onNext(opResult));
    LOG.log(Level.INFO, "Finish executing op: {0}", op);

    // enqueue next operations enabled by the completion of this op
    final Set<Op> nextOps = ongoingPlan.getNextOps(op);

    if (!nextOps.isEmpty()) {
      while (true) {
        try {
          nextOpsToExecuteInParallel.put(nextOps);
          break;
        } catch (InterruptedException e) {
          // ignore and keep waiting
        }
      }
    }
  }

  /**
   * A class for encapsulating the state of plan execution.
   */
  private static final class ExecutingPlan {
    private final ETPlan plan;
    private final int numTotalOps;
    private AtomicInteger numCompletedOpsCounter = new AtomicInteger(0);
    private CountDownLatch completedLatch = new CountDownLatch(1);

    private ExecutingPlan(final ETPlan plan) {
      this.plan = plan;
      this.numTotalOps = plan.getNumTotalOps();
    }

    /**
     * Retrieves the next parallel operations enabled by the completion of this operation.
     * @param completeOp a completed operation
     * @return a set of operations returned by {@link ETPlan#onComplete(Op)}.
     */
    Set<Op> getNextOps(final Op completeOp) {
      final int numCompletedOps = numCompletedOpsCounter.incrementAndGet();
      LOG.log(Level.INFO, "The number of completed operations: [{0} / {1}]",
          new Object[]{numCompletedOps, numTotalOps});

      if (numTotalOps == numCompletedOps) {
        completedLatch.countDown();
      }

      return plan.onComplete(completeOp);
    }

    /**
     * Waits for the completion of the plan.
     */
    void waitPlanCompletion() {
      while (true) {
        try {
          completedLatch.await();
          break;
        } catch (InterruptedException e) {
          // ignore and keep waiting
        }
      }
    }
  }
}
