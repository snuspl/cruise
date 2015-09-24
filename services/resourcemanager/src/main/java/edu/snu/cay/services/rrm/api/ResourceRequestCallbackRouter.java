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

import edu.snu.cay.services.rrm.impl.ResourceRequestCallbackRouterImpl;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.EventHandler;

import javax.annotation.Nullable;

/**
 * Routes callbacks for resource requests by evaluatorId.
 * {@code ResourceRequestManager} registers each callback when {@code ResourceRequestManager.getRequestor()} is called.
 */
@DriverSide
@DefaultImplementation(ResourceRequestCallbackRouterImpl.class)
public interface ResourceRequestCallbackRouter {

  /**
   * Register a new callback for a resource request.
   * @param evaluatorId A unique identifier for the requested evaluator
   * @param callback The handler to be called, or null for a no-op callback
   */
  void register(String evaluatorId,
                @Nullable EventHandler<ActiveContext> callback);

  /**
   * Call the registered callback for a resource request.
   * @param evaluatorId A unique identifier for the requested evaluator
   * @return The handler to be called, or null if nothing is found
   */
  EventHandler<ActiveContext> getCallback(final String evaluatorId);
}
