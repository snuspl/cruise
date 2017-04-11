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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * An implementation of {@link ListenableFuture} that aggregates future results reported by {@link #onCompleted(V)}.
 * {@link List<V>}, the aggregated results are returned through {@link #get()}.
 */
public class AggregateFuture<V> implements ListenableFuture<List<V>> {
  private final List<V> results;
  private final CountDownLatch completedLatch;
  private final Collection<EventHandler<List<V>>> listeners = new LinkedList<>();

  /**
   * Creates an AggregateFuture that collects multiple results.
   * @param numToAggregate the number of results to aggregate
   */
  public AggregateFuture(final int numToAggregate) {
    this.results = new ArrayList<>(numToAggregate);
    this.completedLatch = new CountDownLatch(numToAggregate);
  }

  /**
   * Sets the result of the completed operation.
   */
  public synchronized void onCompleted(final V result) {
    if (isDone()) {
      throw new IllegalStateException("The operation associated with this Future was already complete");
    }

    results.add(result);
    completedLatch.countDown();

    if (isDone()) {
      listeners.forEach(listener -> listener.onNext(results));
      listeners.clear();
    }
  }

  @Override
  public synchronized void addListener(final EventHandler<List<V>> listener) {
    if (isDone()) {
      listener.onNext(results);
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
  public List<V> get() throws InterruptedException, ExecutionException {
    completedLatch.await();
    return results;
  }

  @Override
  public List<V> get(final long timeout, final TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    if (!completedLatch.await(timeout, unit)) {
      throw new TimeoutException("Timeout while waiting for the completion");
    }
    return results;
  }
}
