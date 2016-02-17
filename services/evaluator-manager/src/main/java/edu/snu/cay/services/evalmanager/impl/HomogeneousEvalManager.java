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
 * the following assumptions are reasonable:
 * 1) the driver does not call {@code onEvent()} multiple times
 * for the same {@link AllocatedEvaluator} or {@link ActiveContext} event
 * 2) there are no REEF event handlers which submit context
 * except for the handlers passed by {@code allocateEvaluators}
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
  private final Queue<Tuple2<EventHandler<AllocatedEvaluator>, Queue<EventHandler<ActiveContext>>>> requestQueue;

  /**
   * Maps evaluator id to {@link ActiveContext} event handling plan.
   */
  private final ConcurrentMap<String, Queue<EventHandler<ActiveContext>>> evalIdToHandlersMap;

  /**
   * REEF evaluator requestor.
   */
  private final EvaluatorRequestor evaluatorRequestor;

  /**
   * Memory size of evaluator requests, specified by {@link Parameters.EvaluatorSize}.
   */
  private final int evalSize;

  @Inject
  private HomogeneousEvalManager(final EvaluatorRequestor evaluatorRequestor,
                                 @Parameter(Parameters.EvaluatorSize.class) final int evalSize) {
    this.requestQueue = new ConcurrentLinkedQueue<>();
    this.evalIdToHandlersMap = new ConcurrentHashMap<>();
    this.evaluatorRequestor = evaluatorRequestor;
    this.evalSize = evalSize;
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
      requestQueue.add(new Tuple2<>(evaluatorAllocatedHandler, handlerQueue));
    }
    final EvaluatorRequest request = EvaluatorRequest.newBuilder()
        .setNumber(evalNum)
        .setNumberOfCores(1)
        .setMemory(evalSize)
        .build();
    evaluatorRequestor.submit(request);
  }

  /**
   * {@inheritDoc}
   */
  public void onEvent(final AllocatedEvaluator allocatedEvaluator) {
    LOG.entering(HomogeneousEvalManager.class.getSimpleName(), "onEvent");

    final Tuple2<EventHandler<AllocatedEvaluator>, Queue<EventHandler<ActiveContext>>> handlers = requestQueue.remove();
    evalIdToHandlersMap.put(allocatedEvaluator.getId(), handlers.getT2());
    handlers.getT1().onNext(allocatedEvaluator);

    LOG.exiting(HomogeneousEvalManager.class.getSimpleName(), "onEvent");
  }

  /**
   * {@inheritDoc}
   */
  public void onEvent(final ActiveContext activeContext) {
    LOG.entering(HomogeneousEvalManager.class.getSimpleName(), "onEvent");

    final Queue<EventHandler<ActiveContext>> handlerQueue = evalIdToHandlersMap.get(activeContext.getEvaluatorId());
    if (handlerQueue == null) {
      throw new RuntimeException(String.format("No more ActiveContext handlers for %s", activeContext));
    }

    final EventHandler<ActiveContext> handler = handlerQueue.remove();
    if (handlerQueue.isEmpty()) {
      evalIdToHandlersMap.remove(activeContext.getEvaluatorId());
    }
    handler.onNext(activeContext);

    LOG.exiting(HomogeneousEvalManager.class.getSimpleName(), "onEvent");
  }
}
