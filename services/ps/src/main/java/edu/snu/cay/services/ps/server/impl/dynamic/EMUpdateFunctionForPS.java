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
package edu.snu.cay.services.ps.server.impl.dynamic;

import edu.snu.cay.services.em.evaluator.api.EMUpdateFunction;
import edu.snu.cay.services.ps.server.api.ParameterUpdater;

import javax.inject.Inject;

/**
 * EM update function implementation of PS.
 * It processes given key/value using {@link ParameterUpdater}.
 * @param <K> a type of EM key, which is a {@link HashedKey} that encapsulates PS key
 * @param <V> a type of value
 * @param <A> a type of PS key, which is an internal key of {@link HashedKey} used in EM
 */
public final class EMUpdateFunctionForPS<K, V, A> implements EMUpdateFunction<K, V> {
  private final ParameterUpdater<A, ?, V> parameterUpdater;

  @Inject
  private EMUpdateFunctionForPS(final ParameterUpdater<A, ?, V> parameterUpdater) {
    this.parameterUpdater = parameterUpdater;
  }

  @Override
  public V getInitValue(final K emKey) {
    // PS uses HashedKey for EM key, which embeds actual PS key and exposes it through HashedKey.getKey().
    // So to get actual PS key and use it with ParameterUpdater, we need to cast a given EM key to HashedKey first.
    final A psKey = ((HashedKey<A>) emKey).getKey();
    return parameterUpdater.initValue(psKey);
  }

  @Override
  public V getUpdateValue(final V oldValue, final V deltaValue) {
    return parameterUpdater.update(oldValue, deltaValue);
  }
}
