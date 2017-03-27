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
package edu.snu.cay.services.et.evaluator.api;

/**
 * A class representing the result of table data operation.
 * @param <V> a type of data value
 */
public interface DataOpResult<V> {

  /**
   * Returns an output of the operation.
   * It's null when 1) there's no associated value for the given key or 2) the operation has been failed.
   * Two cases can be distinguished with {@link #isSuccess()}.
   * @return an result of data operation
   */
  V getOutputData();

  /**
   * Returns a boolean that indicates whether the operation is succeeded or not.
   * It's required because null value in {@link #getOutputData()} is a valid case.
   * @return True if it's completed successfully within timeout
   */
  boolean isSuccess();

  /**
   * Waits until an operation that has been sent to remote is completed.
   * @throws InterruptedException
   */
  void waitRemoteOp() throws InterruptedException;

  /**
   * Waits until an operation that has been sent to remote is completed or
   * a given {@code timeout} elapses.
   * @param timeout a timeout
   * @return True if it's completed within timeout
   * @throws InterruptedException
   */
  boolean waitRemoteOp(long timeout) throws InterruptedException;
}
