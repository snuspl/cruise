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

import edu.snu.cay.services.et.avro.OpType;

import java.util.List;

/**
 * A multiple version of {@link DataOpMetadata}.
 */
class MultiKeyDataOpMetadata<K, V, U> extends DataOpMetadata {

  /**
   * Metadata of the operation for multiple version.
   */
  private final List<K> dataKeys;
  private final List<V> dataValues;
  private final List<U> updateValues;

  MultiKeyDataOpMetadata(final String origExecutorId,
                         final long operationId, final OpType operationType,
                         final boolean replyRequired,
                         final String tableId, final int blockId,
                         final List<K> dataKeys,
                         final List<V> dataValues,
                         final List<U> updateValues) {
    super(origExecutorId, operationId, operationType, replyRequired, false, tableId, blockId);
    this.dataKeys = dataKeys;
    this.dataValues = dataValues;
    this.updateValues = updateValues;
  }

  /**
   * @return a list of key
   */
  List<K> getKeys() {
    return dataKeys;
  }

  /**
   * @return a list of values. It's empty collection when the type of operation is not MULTI_PUT
   */
  List<V> getValues() {
    return dataValues;
  }

  /**
   * @return a list of update values.
   */
  List<U> getUpdateValues() {
    return updateValues;
  }
}
