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

import edu.snu.cay.services.em.avro.EMMigrationMsg;
import edu.snu.cay.services.em.driver.impl.EMDeleteExecutorImpl;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.EventHandler;

/**
 * An object which handles EM delete request.
 * EM cannot release an evaluator alone since EM does not know what contexts, services, and tasks
 * are submitted on the evaluator and thus cannot handle closing events of them.
 * Therefore, EM user should define their own EMDeleteExecutor
 * which takes care of components on the evaluator and cleanly release it.
 */
@DefaultImplementation(EMDeleteExecutorImpl.class)
public interface EMDeleteExecutor {

  /**
   * Execute EM delete operation.
   * @param evalId identifier of the evaluator to release
   * @param callback an application-level callback to be called
   * @return true if succeeds to delete
   */
  boolean execute(String evalId, EventHandler<EMMigrationMsg> callback);
}
