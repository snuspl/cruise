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
/**
 * Evaluator Manager classes.
 */
package edu.snu.cay.services.evalmanager.impl;

import edu.snu.cay.services.evalmanager.api.EvaluatorManager;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.impl.Tuple2;

import javax.inject.Inject;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An implementation for {@link EvaluatorManager}.
 * It is capable of requesting heterogeneous evaluators with different CPU cores and memory size.
 * To make it possible, it does not submit a EvaluatorRequest to {@link EvaluatorRequestor},
 * which is incapable of mapping request and result, until the previous request is completed.
 *
 * Since one of the purposes of {@link EvaluatorManager} is to help stacking REEF contexts on evaluators,
 * the following assumptions are reasonable: <br>
 * 1) methods of this class are invoked in the following order: <br>
 * allocateEvaluators() -> onEvaluatorAllocated() -> onContextActive() (can be invoked multiple times) <br>
 * 2) the driver does not call {@code onEvent()} multiple times
 * for the same {@link AllocatedEvaluator} or {@link ActiveContext} event <br>
 * 3) there are no REEF event handlers which submit context
 * except for the handlers passed by {@code allocateEvaluators} <br>
 *
 * With the assumptions above, this class is thread-safe.
 */
@DriverSide
public final class HeterogeneousEvalManager implements EvaluatorManager {
  private static final Logger LOG = Logger.getLogger(HeterogeneousEvalManager.class.getName());

  /**
   * A waiting queue for evaluator-related event handling plans.
   * {@code onEvent(AllocatedEvaluator)} pops an element from this, and assigns it to the evaluator.
   */
  private final Queue<Tuple2<EventHandler<AllocatedEvaluator>, Queue<EventHandler<ActiveContext>>>> pendingEvalRequests;

  /**
   * Maps evaluator id to {@link ActiveContext} event handling plan.
   */
  private final ConcurrentMap<String, Queue<EventHandler<ActiveContext>>> evalIdToPendingContextHandlers;

  private final EvaluatorRequestor evaluatorRequestor;

  /**
   * Only one request can be executed at a time.
   * Following requests wait for the request to be completed.
   * The request will be completed after all requested number of evaluators are allocated.
   */
  private final AtomicReference<CountDownLatch> ongoingEvaluatorRequest = new AtomicReference<>(new CountDownLatch(0));

  @Inject
  HeterogeneousEvalManager(final EvaluatorRequestor evaluatorRequestor) {
    this.pendingEvalRequests = new ConcurrentLinkedQueue<>();
    this.evalIdToPendingContextHandlers = new ConcurrentHashMap<>();
    this.evaluatorRequestor = evaluatorRequestor;
  }

  /**
   * {@inheritDoc}
   */
  public synchronized void allocateEvaluators(final int evalNum, final int megaBytes, final int cores,
                                              final EventHandler<AllocatedEvaluator> evaluatorAllocatedHandler,
                                              final List<EventHandler<ActiveContext>> contextActiveHandlerList) {
    try {
      ongoingEvaluatorRequest.get().await();
    } catch (final InterruptedException e) {
      LOG.log(Level.WARNING, "Interrupted while waiting for ongoing request to be finished", e);
    }

    LOG.log(Level.INFO, "Requesting {0} evaluators...", evalNum);

    for (int i = 0; i < evalNum; i++) {
      final Queue<EventHandler<ActiveContext>> handlerQueue = new ConcurrentLinkedQueue<>(contextActiveHandlerList);
      pendingEvalRequests.add(new Tuple2<>(evaluatorAllocatedHandler, handlerQueue));
    }
    final EvaluatorRequest request = EvaluatorRequest.newBuilder()
        .setNumber(evalNum)
        .setNumberOfCores(cores)
        .setMemory(megaBytes)
        .build();

    ongoingEvaluatorRequest.set(new CountDownLatch(request.getNumber()));
    evaluatorRequestor.submit(request);
  }

  /**
   * {@inheritDoc}
   */
  public void onEvaluatorAllocated(final AllocatedEvaluator allocatedEvaluator) {
    final Tuple2<EventHandler<AllocatedEvaluator>, Queue<EventHandler<ActiveContext>>> handlers
        = pendingEvalRequests.remove();
    final EventHandler<AllocatedEvaluator> evaluatorAllocatedHandler = handlers.getT1();
    final Queue<EventHandler<ActiveContext>> contextActiveHandlers = handlers.getT2();
    if (!contextActiveHandlers.isEmpty()) {
      evalIdToPendingContextHandlers.put(allocatedEvaluator.getId(), contextActiveHandlers);
    }
    evaluatorAllocatedHandler.onNext(allocatedEvaluator);

    ongoingEvaluatorRequest.get().countDown();
  }

  /**
   * {@inheritDoc}
   */
  public void onContextActive(final ActiveContext activeContext) {
    final Queue<EventHandler<ActiveContext>> handlerQueue
        = evalIdToPendingContextHandlers.get(activeContext.getEvaluatorId());
    if (handlerQueue == null) {
      throw new RuntimeException(String.format("No more ActiveContext handlers for %s", activeContext));
    } else {
      // According to the assumption 2) in javadoc of this class,
      // other threads processing this method for the same evaluator do not exist.
      // Also, the handlerQueue stored in evalIdToPendingContextHandlers should have
      // at least 1 element initially due to the behavior of onEvaluatorAllocated().
      // By the assumptions above, handlerQueue must not be empty.
      final EventHandler<ActiveContext> handler = handlerQueue.remove();
      if (handlerQueue.isEmpty()) {
        evalIdToPendingContextHandlers.remove(activeContext.getEvaluatorId());
      }
      handler.onNext(activeContext);
    }
  }
}
