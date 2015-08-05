/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package edu.snu.cay.services.shuffle.evaluator.operator.impl;

import edu.snu.cay.services.shuffle.evaluator.operator.BaseShuffleReceiver;
import edu.snu.cay.services.shuffle.evaluator.operator.PushShuffleReceiver;
import edu.snu.cay.services.shuffle.strategy.ShuffleStrategy;
import org.apache.reef.io.Tuple;

import javax.inject.Inject;
import java.util.List;

/**
 * TODO : implements functionality
 */
public final class PushShuffleReceiverImpl<K, V> implements PushShuffleReceiver<K, V> {

  private final BaseShuffleReceiver<K, V> baseShuffleReceiver;
  @Inject
  private PushShuffleReceiverImpl(
      final BaseShuffleReceiver<K, V> baseShuffleReceiver) {
    this.baseShuffleReceiver = baseShuffleReceiver;
  }

  @Override
  public List<Tuple<K, V>> receive() {
    return null;
  }

  @Override
  public ShuffleStrategy<K> getShuffleStrategy() {
    return baseShuffleReceiver.getShuffleStrategy();
  }

  @Override
  public List<String> getSelectedReceiverIdList(final K key) {
    return baseShuffleReceiver.getSelectedReceiverIdList(key);
  }
}
