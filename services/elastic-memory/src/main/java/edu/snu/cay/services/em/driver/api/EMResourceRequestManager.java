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

import edu.snu.cay.services.em.driver.impl.EMResourceRequestManagerImpl;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.EventHandler;

import javax.annotation.Nullable;

/**
 * Manages callbacks for EM resource requests.
 * {@code ElasticMemory} will use {@code register} method and Driver will use {@code onCompleted} method.
 */
@DriverSide
@DefaultImplementation(EMResourceRequestManagerImpl.class)
public interface EMResourceRequestManager {

  /**
   * Register callback for future allocating evaluator.
   * @param callback an application-level callback to be called, or null if no callback is needed
   */
  void register(@Nullable EventHandler<ActiveContext> callback);

  /**
   * Determine whether this evaluator was requested by ElasticMemory or not,
   * and if given allocatedEvaluator is EM requested evaluator, bind callback to it.
   * Driver will call this method.
   * @return true if ElasticMemory has requested for evaluator
   */
  boolean bindCallback(AllocatedEvaluator allocatedEvaluator);

  /**
   * Trigger registered callback for given activeContext.
   * @param activeContext given activeContext
   */
  void onCompleted(ActiveContext activeContext);
}
