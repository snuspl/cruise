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
package edu.snu.cay.services.em.driver.api;

import edu.snu.cay.services.em.driver.impl.EMResourceCallbackManagerImpl;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.EventHandler;

/**
 * Manages callbacks for EM resource requests.
 * {@code ElasticMemory} will use {@code register} method and Driver will use {@code onCompleted} method.
 */
@DriverSide
@DefaultImplementation(EMResourceCallbackManagerImpl.class)
public interface EMResouceCallbackManager {

  /**
   * Register no-op callback for given evaluatorId.
   * @param evaluatorId evaluator identifier
   */
  void register(String evaluatorId);

  /**
   * Register callback for given evaluatorId.
   * @param evaluatorId evaluator identifier
   * @param callback an application-level callback to be called
   */
  void register(String evaluatorId, EventHandler<ActiveContext> callback);

  /**
   * Trigger registered callback for given activeContext.
   * @param activeContext given activeContext
   */
  void onCompleted(ActiveContext activeContext);
}
