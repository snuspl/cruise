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
package edu.snu.cay.services.et.plan.impl;

import edu.snu.cay.services.et.common.util.concurrent.ListenableFuture;
import edu.snu.cay.services.et.common.util.concurrent.ResultFuture;
import edu.snu.cay.services.et.driver.api.ETMaster;
import edu.snu.cay.services.et.exceptions.PlanAlreadyExecutingException;
import edu.snu.cay.services.et.exceptions.PlanOpExecutionException;
import edu.snu.cay.services.et.plan.api.Op;
import edu.snu.cay.services.et.plan.api.PlanExecutor;

import javax.inject.Inject;
import java.util.Set;
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

  private final ETMaster etMaster;

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

  @Inject
  private PlanExecutorImpl(final ETMaster etMaster) {
    this.etMaster = etMaster;
  }

  @Override
  public ListenableFuture<Void> execute(final ETPlan plan) throws PlanAlreadyExecutingException {
    if (executingPlan.get() != null) {
      throw new PlanAlreadyExecutingException("Executing more than one plan simultaneously is not allowed.");
    }

    // TODO #97: need to validate plan

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

    final ResultFuture<Void> resultFuture = new ResultFuture<>();

    Executors.newSingleThreadExecutor().submit(() -> {

      final int numTotalOps = plan.getNumTotalOps();
      int numStartedOps = 0;

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
          nextOps.forEach(op -> {
            try {
              op.execute(etMaster)
                  .addListener(x -> onOpComplete(op));
            } catch (PlanOpExecutionException e) {
              throw new RuntimeException(e);
            }
          });
          numStartedOps += nextOps.size();
        }
        LOG.log(Level.INFO, "The number of started ops: [{0} / {1}]", new Object[]{numStartedOps, numTotalOps});
      }

      executingPlan.get().waitPlanCompletion();
      resultFuture.onCompleted(null);
      executingPlan.set(null);
    });

    return resultFuture;
  }

  /**
   * Mark the operation as complete and make next operations ready.
   * @param op a completed operation
   */
  private synchronized void onOpComplete(final Op op) {
    final ExecutingPlan ongoingPlan = executingPlan.get();
    if (ongoingPlan == null) {
      throw new RuntimeException("There's no ongoing plan");
    }

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
