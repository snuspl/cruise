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
package edu.snu.cay.services.et.common.util.concurrent;

import org.apache.reef.wake.EventHandler;

import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * An implementation of {@link ListenableFuture} that is completed by {@link #onCompleted(V)}.
 * The result is returned through {@link #get()}.
 */
public class ResultFuture<V> implements ListenableFuture<V> {
  private volatile V result = null;
  private final CountDownLatch completedLatch;
  private final Collection<EventHandler<V>> listeners = new LinkedList<>();

  /**
   * Creates a ResultFuture that holds the result of an operation.
   */
  public ResultFuture() {
    this.completedLatch = new CountDownLatch(1);
  }

  /**
   * Sets the result of the completed operation. Note that this method should be called only once.
   */
  public synchronized void onCompleted(final V aResult) {
    if (isDone()) {
      throw new IllegalStateException("The operation associated with this Future was already complete");
    }

    result = aResult;
    completedLatch.countDown();

    if (isDone()) {
      listeners.forEach(listener -> listener.onNext(result));
      listeners.clear();
    }
  }

  @Override
  public synchronized void addListener(final EventHandler<V> listener) {
    if (isDone()) {
      listener.onNext(result);
    } else {
      listeners.add(listener);
    }
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
  public V get() throws InterruptedException, ExecutionException {
    completedLatch.await();
    return result;
  }

  @Override
  public V get(final long timeout, final TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    if (!completedLatch.await(timeout, unit)) {
      throw new TimeoutException("Timeout while waiting for the completion");
    }
    return result;
  }
}
