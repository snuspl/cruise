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

import edu.snu.cay.services.et.evaluator.api.DataOpResult;

import javax.annotation.Nullable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * An implementation of {@link DataOpResult}.
 * @param <V> a type of data value
 */
class DataOpResultImpl<V> implements DataOpResult<V> {

  /**
   * A latch that will be released when the operation gets result.
   */
  private final CountDownLatch completedLatch;

  /**
   * Result of the operation.
   */
  private volatile boolean isSuccess;
  private volatile V outputData;

  DataOpResultImpl() {
    this.isSuccess = false;
    this.outputData = null;
    this.completedLatch = new CountDownLatch(1);
  }

  DataOpResultImpl(final V outputData, final boolean isSuccess) {
    this.isSuccess = isSuccess;
    this.outputData = outputData;
    this.completedLatch = new CountDownLatch(0);
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

  @Override
  public V getOutputData() {
    return outputData;
  }

  @Override
  public boolean isSuccess() {
    return isSuccess;
  }

  @Override
  public void waitRemoteOp() throws InterruptedException {
    completedLatch.await();
  }

  @Override
  public boolean waitRemoteOp(final long timeout) throws InterruptedException {
    return completedLatch.await(timeout, TimeUnit.MILLISECONDS);
  }
}
