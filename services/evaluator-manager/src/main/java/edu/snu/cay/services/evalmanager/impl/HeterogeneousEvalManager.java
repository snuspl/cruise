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
 * An implementation of {@link EvaluatorManager} that can request
 * heterogeneous evaluators (i.e., with the different number of CPU cores and memory size).
 * Because the current {@link EvaluatorRequestor} does not provide a way to match request and result,
 * EvaluatorRequestor is submitted only after the previous request has been finished.
 *
 * This class is thread-safe under the following assumptions:
 * 1) methods of this class are invoked in the following order:
 * allocateEvaluators() -> onEvaluatorAllocated() -> onContextActive() (can be invoked multiple times)
 * 2) the driver does not call {@code onEvent()} multiple times
 * for the same {@link AllocatedEvaluator} or {@link ActiveContext} event
 * 3) there are no REEF event handlers which submit context
 * except for the handlers passed by {@code allocateEvaluators}
 *
 * The assumptions above are reasonable since one of the {@link EvaluatorManager}'s role is
 * to help stacking REEF contexts on evaluators.
 */
@DriverSide
public final class HeterogeneousEvalManager implements EvaluatorManager {
  private static final Logger LOG = Logger.getLogger(HeterogeneousEvalManager.class.getName());

  /**
   * A waiting queue for evaluator-related event handling plans.
   * {@code onEvent(AllocatedEvaluator)} pops an element from this, and assigns it to the evaluator.
   */
  private final Queue<Tuple2<EventHandler<AllocatedEvaluator>, Queue<EventHandler<ActiveContext>>>> pendingEvalRequests
      = new ConcurrentLinkedQueue<>();

  /**
   * Maps evaluator id to {@link ActiveContext} event handling plan.
   */
  private final ConcurrentMap<String, Queue<EventHandler<ActiveContext>>> evalIdToPendingContextHandlers
      = new ConcurrentHashMap<>();

  /**
   * Only one request can be executed at a time.
   * Following requests wait for the request to be completed.
   * The request will be completed after all requested number of evaluators are allocated.
   */
  private final AtomicReference<CountDownLatch> ongoingEvaluatorRequest = new AtomicReference<>(new CountDownLatch(0));

  private final EvaluatorRequestor evaluatorRequestor;

  @Inject
  private HeterogeneousEvalManager(final EvaluatorRequestor evaluatorRequestor) {
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
