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
package edu.snu.cay.services.evalmanager.api;

import edu.snu.cay.services.evalmanager.impl.HomogeneousEvalManager;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.EventHandler;

import java.util.List;

/**
 * Evaluator Manager API, helps managing REEF events related to evaluator and context.
 * This service is intended to behave as a unified, unique path for requesting evaluators.
 * Different components can request for different plans of event handling.
 * Thus, the user can easily build a stack of REEF context & service,
 * without error-prone if-else statements in their REEF handlers.
 */
@DriverSide
@DefaultImplementation(HomogeneousEvalManager.class)
public interface EvaluatorManager {

  /**
   * Requests for {@code evalNum} evaluators with {@code megaBytes} memory and {@code cores} CPUs.
   * {@code evaluatorAllocatedHandler} and {@code contextActiveHandlerList} specifies
   * the plan for handling REEF events for these requested evaluators.
   * {@link ActiveContext} handlers will be executed with the sequence specified in {@code contextActiveHandlerList},
   * results in well-ordered context & service stack.
   * @param evalNum number of evaluators to request
   * @param megaBytes memory size of each new evaluator in MB
   * @param cores number of cores of each new evaluator
   * @param evaluatorAllocatedHandler plan for handling {@link AllocatedEvaluator} event
   * @param contextActiveHandlerList plan for handling {@link ActiveContext} events
   */
  void allocateEvaluators(int evalNum, int megaBytes, int cores,
                          EventHandler<AllocatedEvaluator> evaluatorAllocatedHandler,
                          List<EventHandler<ActiveContext>> contextActiveHandlerList);

  /**
   * Invokes {@link AllocatedEvaluator} event handler registered by {@code allocateEvaluators()}.
   * @param allocatedEvaluator REEF event to handle
   */
  void onEvaluatorAllocated(AllocatedEvaluator allocatedEvaluator);

  /**
   * Invokes {@link ActiveContext} event handler registered by {@code allocateEvaluators()}.
   * Tracks necessary states internally, and triggers the correct event handler in specified order.
   * @param activeContext REEF event to handle
   */
  void onContextActive(ActiveContext activeContext);
}
