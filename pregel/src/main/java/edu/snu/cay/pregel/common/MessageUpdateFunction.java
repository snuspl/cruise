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
package edu.snu.cay.pregel.common;

import com.google.common.collect.Lists;
import edu.snu.cay.services.et.evaluator.api.UpdateFunction;

import javax.inject.Inject;
import java.util.List;

/**
 * An UpdateFunction that accumulates messages. Any type of messages are allowed.
 * @param <V> vertex type
 * @param <M> message type
 */
public final class MessageUpdateFunction<V, M> implements UpdateFunction<V, List<M>, M> {

  @Inject
  private MessageUpdateFunction() {

  }

  @Override
  public List<M> initValue(final V key) {
    return Lists.newArrayList();
  }

  @Override
  public List<M> updateValue(final V key, final List<M> list, final M msgValue) {
    list.add(msgValue);
    return list;
  }
}
