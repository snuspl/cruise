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

import edu.snu.cay.services.et.configuration.parameters.TableIdentifier;
import edu.snu.cay.services.et.evaluator.api.PartitionFunction;
import edu.snu.cay.services.et.evaluator.api.Table;
import edu.snu.cay.services.et.evaluator.api.TableComponents;
import edu.snu.cay.services.et.evaluator.api.UpdateFunction;
import edu.snu.cay.services.et.exceptions.BlockNotExistsException;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * An implementation of {@link Table}.
 */
@EvaluatorSide
@ThreadSafe
public final class TableImpl<K, V> implements Table<K, V>, TableComponents {
  // currently simply store data that does not belong to local into this map
  // TODO #25: need to implement distributed kv-store and route accesses
  private final ConcurrentMap<K, V> fakeRemoteDataMap = new ConcurrentHashMap<>();

  /**
   * Table identifier.
   */
  private final String tableId;

  /**
   * Local cache for ownership mapping.
   */
  private final OwnershipCache ownershipCache;

  /**
   * Local blocks which this executor owns.
   */
  private final BlockStore<K, V> blockStore;

  /**
   * Partition function that resolves key into block index.
   */
  private final PartitionFunction<K> partitionFunction;

  /**
   * Update function for update operation.
   */
  private final UpdateFunction<K, V> updateFunction;

  @Inject
  private TableImpl(@Parameter(TableIdentifier.class) final String tableId,
                    final OwnershipCache ownershipCache,
                    final BlockStore<K, V> blockStore,
                    final PartitionFunction<K> partitionFunction,
                    final UpdateFunction<K, V> updateFunction) {
    this.tableId = tableId;
    this.ownershipCache = ownershipCache;
    this.blockStore = blockStore;
    this.partitionFunction = partitionFunction;
    this.updateFunction = updateFunction;
  }

  @Override
  public V put(final K key, final V value) {
    try {
      return getBlock(key).put(key, value);
    } catch (BlockNotExistsException e) {
      // TODO #25: need to implement distributed kv-store and route accesses
      return fakeRemoteDataMap.put(key, value);
    }
  }

  @Override
  public V get(final K key) {
    try {
      return getBlock(key).get(key);
    } catch (BlockNotExistsException e) {
      // TODO #25: need to implement distributed kv-store and route accesses
      return fakeRemoteDataMap.get(key);
    }
  }

  @Override
  public V update(final K key, final V deltaValue) {
    try {
      return getBlock(key).update(key, deltaValue);
    } catch (BlockNotExistsException e) {
      // TODO #25: need to implement distributed kv-store and route accesses
      V oldValue = fakeRemoteDataMap.get(key);
      if (oldValue == null) {
        oldValue = updateFunction.initValue(key);
      }
      final V newValue = updateFunction.updateValue(key, oldValue, deltaValue);
      fakeRemoteDataMap.put(key, newValue);
      return newValue;
    }
  }

  @Override
  public V remove(final K key) {
    try {
      return getBlock(key).remove(key);
    } catch (BlockNotExistsException e) {
      // TODO #25: need to implement distributed kv-store and route accesses
      return fakeRemoteDataMap.remove(key);
    }
  }

  /**
   * @return identifier of the corresponding table
   */
  public String getTableId() {
    return tableId;
  }

  /**
   * Get the block containing the specified key.
   */
  private Block<K, V> getBlock(final K key) throws BlockNotExistsException {
    final int blockId = partitionFunction.getBlockId(key);
    return blockStore.get(blockId);
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
