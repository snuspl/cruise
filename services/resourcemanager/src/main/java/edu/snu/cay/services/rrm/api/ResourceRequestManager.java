/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.cay.services.rrm.api;

import edu.snu.cay.services.rrm.impl.ResourceRequestManagerImpl;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.EventHandler;

/**
 * Interface for handling resource requests.
 * Every component should use this class to request for resources.
 */
@DriverSide
@DefaultImplementation(ResourceRequestManagerImpl.class)
public interface ResourceRequestManager {

  /**
   * REEF drivers and services can use this method to request for evaluators.
   * @param requestorId Identifier of requestor
   * @param evaluatorRequest Specifies requested evaluator information
   */
  void request(String requestorId, EvaluatorRequest evaluatorRequest);

  /**
   * REEF drivers and services can use this method to request for evaluators.
   * @param requestorId Identifier of requestor
   * @param evaluatorRequest Specifies requested evaluator information
   * @param callback An application-level callback to be called
   */
  void request(String requestorId, EvaluatorRequest evaluatorRequest, EventHandler<ActiveContext> callback);

  /**
   * Determine which component requested this evaluator.
   * Can be called after AllocatedEvaluator event occurs.
   * Users may use this method to hand over the allocated evaluator to the right service.
   * AllocatedEvaluatorHandler in Driver may call this method.
   * @param allocatedEvaluator Target evaluator we want to know about
   * @return Identifier of requestor which was registered by {@code request}
   */
  String getRequestor(AllocatedEvaluator allocatedEvaluator);

  /**
   * Determine which component requested this evaluator.
   * Can be called after AllocatedEvaluator event occurs.
   * Users may use this method to hand over the evaluator to the right service.
   * Handlers in Driver may call this method.
   * @param evaluatorId Target evaluatorId we want to know about
   * @return Identifier of requestor which was registered by {@code request}
   */
  String getRequestor(String evaluatorId);

  /**
   * Trigger callbacks previously registered by {@code request}.
   * Can be called after ActiveContext event occurs.
   * ActiveContextHandler in Driver may call this method,
   * so user can control when to execute the preregistered callback.
   * @param activeContext Target activeContext to be handled
   */
  void triggerCallback(ActiveContext activeContext);
}
