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
package edu.snu.cay.dolphin.async;

import org.apache.commons.lang.NotImplementedException;

import javax.inject.Inject;
import java.util.List;

/**
 * An {@link ModelAccessor} implementation based on ET.
 * TODO #1066: implement ET model accessor
 */
public class ETModelAccessor<K, P, V> implements ModelAccessor<K, P, V> {
  static final String MODEL_TABLE_ID = "model_table";

  @Inject
  ETModelAccessor() {
  }

  @Override
  public void push(final K key, final P deltaValue) {
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
}
