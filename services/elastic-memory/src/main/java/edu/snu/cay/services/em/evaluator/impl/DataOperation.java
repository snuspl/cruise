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

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A class that represents a single data operation.
 * It maintains metadata and states of the operation during execution.
 */
public final class DataOperation {

  /**
   * Metadata of the operation.
   */
  private final String origEvalId;
  private final String operationId;
  private final DataOpType operationType;
  private final String dataType;
  private final long dataKey;
  private final Object dataValue;

  /**
   * States of the operation.
   */
  private AtomicBoolean finished = new AtomicBoolean(false);
  private boolean isSuccess = false;
  private Object outputData = null;

  /**
   * A monitoring object to notify a client thread waiting for completion of the operation.
   */
  private final Object monitor = new Object();

  /**
   * A constructor for locally requested operation.
   *
   * @param operationId an id of operation
   * @param operationType a type of operation
   * @param dataType a type of data
   * @param dataKey a key of data
   * @param dataValue a value of data
   */
  DataOperation(final String operationId, final DataOpType operationType,
                       final String dataType, final long dataKey, final Object dataValue) {
    this.origEvalId = null;
    this.operationId = operationId;
    this.operationType = operationType;
    this.dataType = dataType;
    this.dataKey = dataKey;
    this.dataValue = dataValue;
  }

  /**
   * A constructor for remotely requested operation.
   *
   * @param origEvalId an id of original evaluator where the operation is generated
   * @param operationId an id of operation
   * @param operationType a type of operation
   * @param dataType a type of data
   * @param dataKey a key of data
   * @param dataValue a value of data
   */
  DataOperation(final String origEvalId, final String operationId, final DataOpType operationType,
                       final String dataType, final long dataKey, final Object dataValue) {
    this.origEvalId = origEvalId;
    this.operationId = operationId;
    this.operationType = operationType;
    this.dataType = dataType;
    this.dataKey = dataKey;
    this.dataValue = dataValue;
  }

  /**
   * @return true if the local client requested this operation.
   */
  public boolean isFromLocalClient() {
    return origEvalId == null;
  }

  /**
   * @return an id of evaluator that initially requests the operation.
   */
  public String getOrigEvalId() {
    return origEvalId;
  }

  /**
   * @return an operation id that is issued by local memory store.
   */
  public String getOperationId() {
    return operationId;
  }

  /**
   * @returns a type of the operation.
   */
  public DataOpType getOperationType() {
    return operationType;
  }

  /**
   * @returns a type of data what the operation targets.
   */
  public String getDataType() {
    return dataType;
  }

  /**
   * @return a key of data what the operation targets.
   */
  public long getDataKey() {
    return dataKey;
  }

  /**
   * Returns a value of input data for PUT operation.
   * It returns null for GET or REMOVE operations.
   * @return a value of input data.
   */
  public Object getInputData() {
    return dataValue;
  }

  /**
   * Returns true if the operation is finished.
   */
  public boolean isFinished() {
    return finished.get();
  }

  /**
   * Set the result of the operation.
   */
  public void setResult(final boolean success, final Object data) {
    this.isSuccess = success;
    this.outputData = data;
    finished.set(true);
  }

  /**
   * Set the result of the operation and wake the waiting thread.
   */
  public void setResultAndWakeupClientThread(final boolean success, final Object data) {
    setResult(success, data);

    synchronized (monitor) {
      monitor.notify();
    }
  }

  /**
   * Returns true if the operation is succeeded.
   */
  public boolean isSuccess() {
    return isSuccess;
  }

  /**
   * Get the output data of GET operation.
   */
  public Object getOutputData() {
    return outputData;
  }

  /**
   * Start waiting for completion of the operation within a bounded time.
   * @param timeout a maximum waiting time in the milliseconds
   * @throws InterruptedException an exception for interrupts in waiting
   */
  public void waitOperation(final long timeout) throws InterruptedException {
    if (!finished.get()) {
      synchronized (monitor) {
        monitor.wait(timeout);
      }
    }
  }
}
