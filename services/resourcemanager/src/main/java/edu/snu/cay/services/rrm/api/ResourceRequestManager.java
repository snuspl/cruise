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
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.EventHandler;

import javax.annotation.Nullable;

/**
 * Interface for handling resource requests.
 * Every component should use this class to request for resources.
 */
@DriverSide
@DefaultImplementation(ResourceRequestManagerImpl.class)
public interface ResourceRequestManager {

  /**
   * REEF services can use this method to request for evaluators.
   * @param requestorId Identifier of requestor
   * @param number Number of evaluators requested
   * @param megaBytes Memory size of evaluator
   * @param cores Number of cores for each evaluator
   * @param callback An application-level callback to be called
   */
  void request(String requestorId, int number, int megaBytes, int cores,
               @Nullable EventHandler<ActiveContext> callback);

  /**
   * Determine which component requested this evaluator.
   * Can be called after AllocatedEvaluator event occurs.
   * Users may use this method to hand over the allocated evaluator to the right service.
   * @param allocatedEvaluator target evaluator we want to know about
   * @return identifier of requestor which was registered by {@code request}
   */
  String getRequestor(AllocatedEvaluator allocatedEvaluator);

  /**
   * Trigger callbacks previously registered by {@code request}.
   * Can be called after ActiveContext event occurs.
   * @param activeContext target activeContext to be handled
   */
  void triggerCallback(ActiveContext activeContext);
}
