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
package edu.snu.cay.services.et.evaluator.impl;

import edu.snu.cay.services.et.evaluator.api.Table;
import edu.snu.cay.services.et.evaluator.api.TableComponents;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An implementation class of table.
 * It should be injected based on {@link org.apache.reef.tang.Configuration} of a table.
 * @param <K>
 * @param <V>
 */
public final class TableImpl<K, V> implements Table<K, V>, TableComponents {

  private final OwnershipCache ownershipCache;
  private final BlockStore blockStore;

  // currently simply store data into local map
  // TODO #25: need to implement distributed kv-store and route accesses
  private final Map<K, V> dataMap = new ConcurrentHashMap<>();

  @Inject
  private TableImpl(final OwnershipCache ownershipCache, final BlockStore blockStore) {
    this.ownershipCache = ownershipCache;
    this.blockStore = blockStore;
  }

  @Override
  public V put(final K key, final V value) {
    return dataMap.put(key, value);
  }

  @Override
  public V get(final K key) {
    return dataMap.get(key);
  }

  @Override
  public V update(final K key, final V deltaValue) {
    return null;
  }

  @Override
  public OwnershipCache getOwnershipCache() {
    return ownershipCache;
  }

  @Override
  public BlockStore getBlockStore() {
    return blockStore;
  }
}
