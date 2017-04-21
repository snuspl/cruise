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

import edu.snu.cay.services.et.evaluator.api.BlockPartitioner;
import edu.snu.cay.services.et.evaluator.api.UpdateFunction;

import javax.inject.Inject;

/**
 * A private component for allowing access to internal components of a table.
 */
final class TableComponents<K, V, U> {

  /**
   * Local cache for ownership mapping.
   */
  private final OwnershipCache ownershipCache;

  /**
   * Local blocks which this executor owns.
   */
  private final BlockStore<K, V, U> blockStore;

  /**
   * A serializer for both key and value of a table.
   */
  private final KVUSerializer<K, V, U> kvuSerializer;

  /**
   * A function for updating values in the table.
   */
  private final UpdateFunction<K, V, U> updateFunction;

  /**
   * Partition function that resolves key into block id.
   */
  private final BlockPartitioner<K> blockPartitioner;

  @Inject
  private TableComponents(final OwnershipCache ownershipCache,
                          final BlockStore<K, V, U> blockStore,
                          final KVUSerializer<K, V, U> kvuSerializer,
                          final UpdateFunction<K, V, U> updateFunction,
                          final BlockPartitioner<K> blockPartitioner) {
    this.ownershipCache = ownershipCache;
    this.blockStore = blockStore;
    this.kvuSerializer = kvuSerializer;
    this.updateFunction = updateFunction;
    this.blockPartitioner = blockPartitioner;
  }

  OwnershipCache getOwnershipCache() {
    return ownershipCache;
  }

  BlockStore<K, V, U> getBlockStore() {
    return blockStore;
  }

  UpdateFunction<K, V, U> getUpdateFunction() {
    return updateFunction;
  }

  KVUSerializer<K, V, U> getSerializer() {
    return kvuSerializer;
  }

  BlockPartitioner<K> getBlockPartitioner() {
    return blockPartitioner;
  }
}
