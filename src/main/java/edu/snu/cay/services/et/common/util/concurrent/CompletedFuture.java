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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * An implementation of {@link ListenableFuture} represents that it's already completed.
 * Listeners added by {@link #addListener} are invoked immediately,
 * because it's already completed.
 */
public final class CompletedFuture<V> implements ListenableFuture<V> {

  private final V v;

  /**
   * Creates a CompletedFuture that holds the result of a completed operation.
   * @param v a value to be returned on {@link #get()}
   */
  public CompletedFuture(final V v) {
    this.v = v;
  }

  @Override
  public void addListener(final EventHandler<V> listener) {
    listener.onNext(v);
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
    return true;
  }

  @Override
  public V get() throws InterruptedException, ExecutionException {
    return v;
  }

  @Override
  public V get(final long timeout, final TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return v;
  }
}
