/*
 *
 *  * Copyright (C) 2016 Seoul National University
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *         http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package edu.snu.cay.services.em.evaluator;

import edu.snu.cay.services.em.avro.DataOpType;
import org.apache.reef.io.network.util.Pair;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A class that represents a single data operation.
 * It maintains metadata and state of the operation during execution.
 */
public final class DataOperation {

  // metadata of the operation
  private final String origEvalId; // null when the operation is requested from a local client
  private final String operationId;
  private final DataOpType operationType;
  private final String dataType;
  private final long dataKey;
  private final Object dataValue;

  // states of the operation
  private AtomicBoolean remote = new AtomicBoolean(false);
  private AtomicBoolean finished = new AtomicBoolean(false);
  private boolean result = false;
  private Object outputData = null;

  /**
   * A monitoring byte to notify a client thread waiting for completion of operation.
   */
  private final Byte monitor = new Byte("0");

  public DataOperation(final String origEvalId, final String operationId, final DataOpType operationType,
                       final String dataType, final long dataKey, final Object dataValue) {
    this.origEvalId = origEvalId;
    this.operationId = operationId;
    this.operationType = operationType;
    this.dataType = dataType;
    this.dataKey = dataKey;
    this.dataValue = dataValue;
  }

  /**
   * Returns a boolean that represents whether the operation is requested from a local client or not.
   */
  public boolean isLocalRequest() {
    return origEvalId == null;
  }

  /**
   * Returns an id of evaluator that initially requests the operation.
   */
  public String getOrigEvalId() {
    return origEvalId;
  }

  /**
   * eturns an operation id that is issued by local memory store.
   */
  public String getOperationId() {
    return operationId;
  }

  /**
   * Returns a type of the operation.
   */
  public DataOpType getOperationType() {
    return operationType;
  }

  /**
   * Returns a type of data what the operation targets.
   */
  public String getDataType() {
    return dataType;
  }

  /**
   * Returns a key of data what the operation targets.
   */
  public long getDataKey() {
    return dataKey;
  }

  /**
   * Returns a value of input data. It has a valid value only for the PUT operation.
   */
  public Object getDataValue() {
    return dataValue;
  }

  /**
   * Returns a boolean that represents whether the operation is finished or not.
   */
  public boolean isFinished() {
    return finished.get();
  }

  /**
   * Returns a boolean that represents whether the operation needs to be done in a remote memory store or
   * can be done in a local memory store. It is determined by the routing result.
   */
  public boolean isRemote() {
    return remote.get();
  }

  /**
   * Change the operation state from local phase to remote phase.
   */
  public void setRemote() {
    remote.set(true);
  }

  /**
   * Set the result of the operation.
   */
  public void setResult(final boolean success, final Object data) {
    this.result = success;
    this.outputData = data;
    finished.set(true);
  }

  /**
   * Set the result of the operation and wake the waiting thread.
   */
  public void setResultAndNotifyClientThread(final boolean success, final Object data) {
    this.result = success;
    this.outputData = data;

    synchronized (monitor) {
      finished.set(true);
      monitor.notify();
    }
  }

  /**
   * Get the result retrieved from the local operation.
   */
  public Pair<Boolean, Object> getResult() {
    return new Pair<>(result, outputData);
  }

  /**
   * Start waiting for completion of the operation within a bounded time.
   * @param timeout a maximum waiting time in the milliseconds
   * @throws InterruptedException an exception for interrupts in waiting
   */
  public void waitOperation(final long timeout) throws InterruptedException {
    synchronized (monitor) {
      if (!finished.get()) {
        monitor.wait(timeout);
      }
    }
  }
}
