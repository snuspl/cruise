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

import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import org.apache.commons.lang.NotImplementedException;
import org.apache.reef.annotations.audience.EvaluatorSide;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * A SSP Parameter Server worker that interacts with servers and the driver.
 * A single instance of this class can be used by more than one thread safely, if and only if
 * the Codec classes are thread-safe.
 */
@EvaluatorSide
public final class SSPParameterWorkerImpl<K, P, V> implements ParameterWorker<K, P, V> {

  @Inject
  private SSPParameterWorkerImpl() {

  }

  @Override
  public void push(final K key, final P preValue) {
    throw new NotImplementedException();
  }

  @Override
  public V pull(final K key) {
    throw new NotImplementedException();
  }

  @Override
  public List<V> pull(final List<K> keys) {
    throw new NotImplementedException();
  }

  @Override
  public void clock() {
    throw new NotImplementedException();
  }

  @Override
  public void close(final long timeoutMs) throws InterruptedException, TimeoutException, ExecutionException {
    throw new NotImplementedException();
  }
}
