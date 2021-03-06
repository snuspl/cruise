/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.spl.cruise.services.et.plan.api;

import edu.snu.spl.cruise.services.et.common.util.concurrent.ListenableFuture;
import edu.snu.spl.cruise.services.et.exceptions.PlanAlreadyExecutingException;
import edu.snu.spl.cruise.services.et.plan.impl.ETPlan;
import edu.snu.spl.cruise.services.et.plan.impl.OpResult;
import edu.snu.spl.cruise.services.et.plan.impl.PlanExecutorImpl;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.EventHandler;

/**
 * A plan executor interface.
 */
@DefaultImplementation(PlanExecutorImpl.class)
public interface PlanExecutor {

  /**
   * Execute a plan asynchronously.
   * Callers can obtain the result through a returned future.
   * Note that in default it does not support concurrent execution of multiple plans.
   *
   * @param plan to execute
   * @return a {@link ListenableFuture} that completes when the plan execution finishes
   * @throws PlanAlreadyExecutingException when it's already executing the other plan
   */
  ListenableFuture<Void> execute(ETPlan plan) throws PlanAlreadyExecutingException;

  /**
   * Registers a listener to be called on the completion of each operation.
   * @param id an identifier of listener
   * @param callback a callback of {@link OpResult}
   */
  void registerListener(String id, EventHandler<OpResult> callback);

  /**
   * Unregisters a listener with {@code id}, if it exists.
   * @param id an identifier of listener
   */
  void unregisterListener(String id);
}
