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
package edu.snu.cay.services.ps.worker.impl;

import edu.snu.cay.services.ps.worker.api.ParameterAccessor;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * A Parameter accessor for a worker thread.
 * This interacts with local caches(thread, worker) and the servers to provide or fetch parameters.
 * This is used to connect the a worker thread
 * to a {@link edu.snu.cay.services.ps.worker.impl.SSPParameterWorkerImpl}.
 *
 * @param <K> class type of parameter keys
 * @param <P> class type of parameter values before they are processed at the servers
 * @param <V> class type of parameter values after they are processed at the servers
 */
public final class ParameterAccessorImpl<K, P, V> implements ParameterAccessor<K, P, V> {

  @Inject
  public ParameterAccessorImpl() {

  }

  @Override
  public void push(final K key, final P preValue) {

  }

  @Override
  public void flush() {

  }

  @Override
  public V pull(final K key) {
    return null;
  }

  @Override
  public List<V> pull(final List<K> keys) {
    return null;
  }

  @Override
  public void close(final long timeoutMs) throws InterruptedException, TimeoutException, ExecutionException {

  }
}
