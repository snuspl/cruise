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

import javax.annotation.Nullable;

/**
 * An abstraction of each access to a remote table.
 */
final class RemoteDataOp<K, V, U> {
  /**
   * The executor id where this operation is heading for.
   */
  private final String targetId;

  /**
   * Metadata of operation.
   */
  private final DataOpMetadata<K, V, U> dataOpMetadata;

  /**
   * Result of the operation.
   */
  private final DataOpResult<V> dataOpResult;

  /**
   * A constructor for an operation.
   * @param origExecutorId an id of the original evaluator where the operation is generated.
   * @param targetId the executor id where this operation is heading for
   * @param operationId an id of operation
   * @param operationType a type of operation
   * @param replyRequired a boolean representing whether this operation requires reply or not
   * @param tableId an id of table
   * @param blockId an id of block
   * @param encodedKey an {@link EncodedKey} of a data key
   * @param dataValue a value of data. It is null when the operation is one of GET, UPDATE, or REMOVE.
   * @param updateValue a value of data. It is not null only when the operation is UPDATE.
   */
  RemoteDataOp(final String origExecutorId, final String targetId,
               final long operationId, final OpType operationType,
               final boolean replyRequired,
               final String tableId, final int blockId,
               final EncodedKey<K> encodedKey, @Nullable final V dataValue, @Nullable final U updateValue) {
    this.targetId = targetId;
    this.dataOpMetadata = new DataOpMetadata<>(origExecutorId, operationId, operationType, replyRequired,
        tableId, blockId, encodedKey, dataValue, updateValue);
    this.dataOpResult = new DataOpResult<>();
  }

  /**
   * @return the executor id where this operation is heading for
   */
  String getTargetId() {
    return targetId;
  }

  /**
   * @return a {@link DataOpMetadata}
   */
  DataOpMetadata<K, V, U> getMetadata() {
    return dataOpMetadata;
  }

  /**
   * @return a {@link DataOpResult}
   */
  DataOpResult<V> getDataOpResult() {
    return dataOpResult;
  }
}
