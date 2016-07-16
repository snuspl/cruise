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

import edu.snu.cay.services.ps.worker.api.AsyncWorkerHandler;

import javax.inject.Inject;

public final class AsyncWorkerHandlerImpl<K, P, V> implements AsyncWorkerHandler<K, P, V> {
  private final ParameterWorkerImpl<K, P, V> parameterWorker;

  @Inject
  private AsyncWorkerHandlerImpl(final ParameterWorkerImpl<K, P, V> parameterWorker) {
    this.parameterWorker = parameterWorker;
  }

  @Override
  public void processPullReply(final K key, final V value) {
    parameterWorker.processPullReply(key, value);
  }

  @Override
  public void processPullReject(final K key) {
    parameterWorker.processPullReject(key);
  }

  @Override
  public void processPushReject(final K key, final P preValue) {
    parameterWorker.push(key, preValue);
  }
}
