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
package edu.snu.spl.cruise.services.et.evaluator.impl;

import edu.snu.spl.cruise.services.et.evaluator.api.BlockPartitioner;
import edu.snu.spl.cruise.services.et.evaluator.api.BulkDataLoader;
import edu.snu.spl.cruise.services.et.evaluator.api.UpdateFunction;
import org.apache.reef.tang.Configuration;

import javax.inject.Inject;

/**
 * A private component for allowing access to internal components of a table.
 */
final class TableComponents<K, V, U> {

  /**
   * A table conifiguration.
   */
  private Configuration tableConf;

  /**
   * A metadata of table.
   */
  private final TableMetadata tableMetadata;

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

  private final BulkDataLoader bulkDataLoader;

  /**
   * Partition function that resolves key into block id.
   */
  private final BlockPartitioner<K> blockPartitioner;

  @Inject
  private TableComponents(final OwnershipCache ownershipCache,
                          final TableMetadata tableMetadata,
                          final BlockStore<K, V, U> blockStore,
                          final KVUSerializer<K, V, U> kvuSerializer,
                          final UpdateFunction<K, V, U> updateFunction,
                          final BulkDataLoader bulkDataLoader,
                          final BlockPartitioner<K> blockPartitioner) {
    this.tableMetadata = tableMetadata;
    this.ownershipCache = ownershipCache;
    this.blockStore = blockStore;
    this.kvuSerializer = kvuSerializer;
    this.updateFunction = updateFunction;
    this.bulkDataLoader = bulkDataLoader;
    this.blockPartitioner = blockPartitioner;
  }

  TableMetadata getTableMetadata() {
    return tableMetadata;
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

  BulkDataLoader getBulkDataLoader() {
    return bulkDataLoader;
  }

  Configuration getTableConf() {
    return tableConf;
  }

  /**
   * It should be called only once when instantiating a table.
   * @param tableConf a table configuration.
   */
  void setTableConf(final Configuration tableConf) {
    this.tableConf = tableConf;
  }
}
