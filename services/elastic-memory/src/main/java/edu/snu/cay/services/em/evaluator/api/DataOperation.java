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
package edu.snu.cay.services.em.evaluator.api;

import edu.snu.cay.services.em.avro.DataOpType;
import org.apache.reef.util.Optional;

/**
 * DataOperation handled in MemoryStore.
 */
public interface DataOperation {

  /**
   * @return an Optional with the id of evaluator that initially requested the operation
   */
  Optional<String> getOrigEvalId();

  /**
   * @return an operation id issued by its origin memory store
   */
  String getOpId();

  /**
   * @return a type of the operation
   */
  DataOpType getOpType();

  /**
   * @return a type of data
   */
  String getDataType();

  /**
   * @return true if the operation is requested from the local client
   */
  boolean isFromLocalClient();

  /**
   * Starts waiting for the completion of the operation within a bounded time.
   * Sub classes should provide a way to wake it up.
   * @param timeout a maximum waiting time in the milliseconds
   */
  boolean waitRemoteOps(long timeout) throws InterruptedException;
}
