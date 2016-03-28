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
package edu.snu.cay.services.em.evaluator.impl;

import edu.snu.cay.services.em.avro.DataOpType;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.util.Optional;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A class that represents a single data operation.
 * It maintains metadata and states of the operation during execution.
 */
@Private
public final class DataOperation<T> {

  /**
   * Metadata of the operation.
   */
  private final Optional<String> origEvalId;
  private final String operationId;
  private final DataOpType operationType;
  private final String dataType;
  private final long dataKey;
  private final Optional<T> dataValue;

  /**
   * States of the operation.
   */
  private final AtomicBoolean finished = new AtomicBoolean(false);
  private final AtomicBoolean isSuccess = new AtomicBoolean(false);
  private final AtomicReference<Optional<T>> outputData = new AtomicReference<>(Optional.<T>empty());

  /**
   * A monitoring object to notify a client thread waiting for completion of the operation.
   */
  private final Object monitor = new Object();

  /**
   * A constructor for an operation.
   *
   * @param origEvalId an Optional with the id of the original evaluator where the operation is generated.
   *                   It is empty when the operation is requested from the local client.
   * @param operationId an id of operation
   * @param operationType a type of operation
   * @param dataType a type of data
   * @param dataKey a key of data
   * @param dataValue an Optional with the value of data. It is empty when the operation is one of GET or REMOVE.
   */
  DataOperation(final Optional<String> origEvalId, final String operationId, final DataOpType operationType,
                final String dataType, final long dataKey, final Optional<T> dataValue) {
    this.origEvalId = origEvalId;
    this.operationId = operationId;
    this.operationType = operationType;
    this.dataType = dataType;
    this.dataKey = dataKey;
    this.dataValue = dataValue;
  }

  /**
   * @return true if the operation is requested from the local client
   */
  boolean isFromLocalClient() {
    return !origEvalId.isPresent();
  }

  /**
   * @return an Optional with the id of evaluator that initially requested the operation
   */
  Optional<String> getOrigEvalId() {
    return origEvalId;
  }

  /**
   * @return an operation id issued by its origin memory store
   */
  String getOperationId() {
    return operationId;
  }

  /**
   * @returns a type of the operation
   */
  DataOpType getOperationType() {
    return operationType;
  }

  /**
   * @returns a type of data
   */
  String getDataType() {
    return dataType;
  }

  /**
   * @return a key of data
   */
  long getDataKey() {
    return dataKey;
  }

  /**
   * Returns an Optional with the value of input data for PUT operation.
   * It returns an empty Optional for GET and REMOVE operations.
   * @return an Optional with input data
   */
  Optional<T> getDataValue() {
    return dataValue;
  }

  /**
   * Sets the result of the operation.
   */
  void setResult(final boolean success, final Optional<T> data) {
    this.isSuccess.set(success);
    this.outputData.set(data);
    finished.set(true);
  }

  /**
   * Sets the result of the operation and wakes the waiting thread.
   */
  void setResultAndNotifyClient(final boolean success, final Optional<T> data) {
    setResult(success, data);

    synchronized (monitor) {
      monitor.notify();
    }
  }

  /**
   * @return true if the operation succeeded
   */
  boolean isSuccess() {
    return isSuccess.get();
  }

  /**
   * Returns an Optional with the output data of the operation.
   * It returns an empty Optional for PUT operation.
   * @return an Optional with the output data
   */
  Optional<T> getOutputData() {
    return outputData.get();
  }

  /**
   * Starts waiting for completion of the operation within a bounded time.
   * @param timeout a maximum waiting time in the milliseconds
   * @throws InterruptedException an exception for interrupts in waiting
   */
  void waitOperation(final long timeout) throws InterruptedException {
    if (!finished.get()) {
      synchronized (monitor) {
        monitor.wait(timeout);
      }
    }
  }
}
