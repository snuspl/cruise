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

import edu.snu.cay.services.et.exceptions.DataAccessFailedException;

import java.util.Map;
import java.util.concurrent.*;

/**
 * The multi-key version of {@link DataOpResult}.
 */
public class MultiKeyDataOpResult<K, V> implements DataOpResult<Map<K, V>> {

  /**
   * A latch that will be released when all the sub-operations get result.
   */
  private final CountDownLatch completedLatch;
  private volatile boolean isSuccess;
  private volatile Map<K, V> resultData;


  MultiKeyDataOpResult(final int numKeys) {
    this.resultData = new ConcurrentHashMap<>();
    this.isSuccess = true;
    this.completedLatch = new CountDownLatch(numKeys);
  }

  @Override
  public synchronized void onCompleted(final Map<K, V> subResult, final boolean success) {
    if (isDone()) {
      throw new IllegalStateException("The operation associated with this Future was already complete");
    }

    resultData.putAll(subResult);
    this.isSuccess = this.isSuccess && success;
    completedLatch.countDown();
  }

  @Override
  public boolean cancel(final boolean mayInterruptIfRunning) {
    return false;
  }

  @Override
  public boolean isCancelled() {
    return false;
  }

  @Override
  public boolean isDone() {
    return completedLatch.getCount() == 0;
  }

  @Override
  public Map<K, V> get() throws InterruptedException, ExecutionException {
    completedLatch.await();
    if (!isSuccess) {
      throw new DataAccessFailedException("Fail to execute table access operation");
    }
    return resultData;
  }

  @Override
  public Map<K, V> get(final long timeout, final TimeUnit timeUnit)
      throws InterruptedException, ExecutionException, TimeoutException {
    if (!completedLatch.await(timeout, timeUnit)) {
      throw new TimeoutException("Timeout while waiting for the completion");
    }

    if (!isSuccess) {
      throw new DataAccessFailedException("Fail to execute table access operation");
    }
    return resultData;
  }
}
