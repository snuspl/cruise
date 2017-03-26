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
package edu.snu.cay.services.et.evaluator.impl;

import edu.snu.cay.services.et.avro.OpType;

import javax.annotation.Nullable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * An abstraction of each access to a remote table.
 */
final class RemoteDataOp<K, V, U> {

  /**
   * Metadata of operation.
   */
  private final DataOpMetadata<K, V, U> dataOp;

  /**
   * A latch that will be released when the operation gets result.
   */
  private final CountDownLatch completedLatch = new CountDownLatch(1);

  /**
   * Result of the operation.
   */
  private volatile boolean isSuccess = false;
  private volatile V outputData = null;

  /**
   * A constructor for an operation.
   * @param origExecutorId an id of the original evaluator where the operation is generated.
   * @param operationId an id of operation
   * @param operationType a type of operation
   * @param tableId an id of table
   * @param blockId an id of block
   * @param dataKey a key of data
   * @param dataValue a value of data. It is null when the operation is one of GET or REMOVE.
   */
  RemoteDataOp(final String origExecutorId,
               final long operationId, final OpType operationType,
               final String tableId, final int blockId,
               final K dataKey, @Nullable final V dataValue, @Nullable final U updateValue) {
    this.dataOp = new DataOpMetadata<>(origExecutorId, operationId, operationType,
        tableId, blockId, dataKey, dataValue, updateValue);
  }

  /**
   * @return a {@link DataOpMetadata}
   */
  DataOpMetadata<K, V, U> getMetadata() {
    return dataOp;
  }

  /**
   * Commit the result of operation.
   * It releases a latch in {@link #waitRemoteOp(long)}.
   * @param output an output data
   * @param success a boolean that indicates whether the operation is succeeded or not
   */
  void commitResult(@Nullable final V output, final boolean success) {
    outputData = output;
    isSuccess = success;
    completedLatch.countDown();
  }

  /**
   * Returns an output of the operation.
   * It's null when 1) there's no associated value for the given key or 2) the operation has been failed.
   * Two cases can be distinguished with {@link #isSuccess()}.
   * @return an result of data operation
   */
  V getOutputData() {
    return outputData;
  }

  /**
   * Returns a boolean that indicates whether the operation is succeeded or not.
   * It's required because null value in {@link #getOutputData()} is a valid case.
   * @return True if it's completed successfully within timeout
   */
  boolean isSuccess() {
    return isSuccess;
  }

  /**
   * Waits until an operation that has been sent to remote is completed.
   * @param timeout a timeout
   * @return True if it's completed within timeout
   * @throws InterruptedException
   */
  boolean waitRemoteOp(final long timeout) throws InterruptedException {
    return completedLatch.await(timeout, TimeUnit.MILLISECONDS);
  }
}
