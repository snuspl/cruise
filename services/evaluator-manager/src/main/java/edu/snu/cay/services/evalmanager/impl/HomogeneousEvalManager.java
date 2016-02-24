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

import edu.snu.cay.common.param.Parameters;
import edu.snu.cay.services.evalmanager.api.EvaluatorManager;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.impl.Tuple2;

import javax.inject.Inject;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An implementation for {@link EvaluatorManager}.
 * Only requests for homogeneous evaluators, with 1 core and {@link Parameters.EvaluatorSize} MBs of memory.
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
public final class HomogeneousEvalManager implements EvaluatorManager {
  private static final Logger LOG = Logger.getLogger(HomogeneousEvalManager.class.getName());

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

  private final int evalMemSizeInMB;

  @Inject
  HomogeneousEvalManager(final EvaluatorRequestor evaluatorRequestor,
                         @Parameter(Parameters.EvaluatorSize.class) final int evalMemSizeInMB) {
    this.pendingEvalRequests = new ConcurrentLinkedQueue<>();
    this.evalIdToPendingContextHandlers = new ConcurrentHashMap<>();
    this.evaluatorRequestor = evaluatorRequestor;
    this.evalMemSizeInMB = evalMemSizeInMB;
  }

  /**
   * {@inheritDoc}
   * {@code contextActiveHandlerList} should have at least 1 element.
   */
  public void allocateEvaluators(final int evalNum,
                                 final EventHandler<AllocatedEvaluator> evaluatorAllocatedHandler,
                                 final List<EventHandler<ActiveContext>> contextActiveHandlerList) {
    LOG.log(Level.INFO, "Requesting {0} evaluators...", evalNum);

    for (int i = 0; i < evalNum; i++) {
      final Queue<EventHandler<ActiveContext>> handlerQueue
          = new ArrayBlockingQueue<>(contextActiveHandlerList.size(), false, contextActiveHandlerList);
      pendingEvalRequests.add(new Tuple2<>(evaluatorAllocatedHandler, handlerQueue));
    }
    final EvaluatorRequest request = EvaluatorRequest.newBuilder()
        .setNumber(evalNum)
        .setNumberOfCores(1)
        .setMemory(evalMemSizeInMB)
        .build();
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
    evalIdToPendingContextHandlers.put(allocatedEvaluator.getId(), contextActiveHandlers);
    evaluatorAllocatedHandler.onNext(allocatedEvaluator);
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
      // Also, handlerQueue should have at least 1 element initially
      // due to the constraint in javadoc of allocateEvaluators().
      // By the assumptions above, handlerQueue must not be empty.
      final EventHandler<ActiveContext> handler = handlerQueue.remove();
      if (handlerQueue.isEmpty()) {
        evalIdToPendingContextHandlers.remove(activeContext.getEvaluatorId());
      }
      handler.onNext(activeContext);
    }
  }
}
