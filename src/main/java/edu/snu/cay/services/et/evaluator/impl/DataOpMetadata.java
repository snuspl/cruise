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
import java.util.Optional;

/**
 * A metadata of each access to a table.
 */
class DataOpMetadata<K, V, U> {

  /**
   * Metadata of the operation.
   */
  private final String origExecutorId;
  private final long operationId;
  private final OpType operationType;
  private final boolean replyRequired;
  private final String tableId;
  private final int blockId;
  private final K dataKey;
  private final EncodedKey<K> encodedKey;
  private final V dataValue;
  private final U updateValue;

  /**
   * A constructor for an operation.
   * @param origExecutorId an id of the original evaluator where the operation is generated.
   * @param operationId an id of operation
   * @param operationType a type of operation
   * @param replyRequired a boolean representing that the operation requires reply or not
   * @param tableId an id of table
   * @param blockId an id of block
   * @param dataKey a key of data
   * @param dataValue a value of data. It is null when the operation is one of GET or REMOVE.
   * @param updateValue a value to be used in {@link edu.snu.cay.services.et.evaluator.api.UpdateFunction},
   *                    which is not null only when UPDATE.
   */
  DataOpMetadata(final String origExecutorId,
                 final long operationId, final OpType operationType,
                 final boolean replyRequired,
                 final String tableId, final int blockId,
                 final K dataKey,
                 @Nullable final V dataValue,
                 @Nullable final U updateValue) {
    this.origExecutorId = origExecutorId;
    this.operationId = operationId;
    this.operationType = operationType;
    this.replyRequired = replyRequired;
    this.tableId = tableId;
    this.blockId = blockId;
    this.dataKey = dataKey;
    this.encodedKey = null;
    this.dataValue = dataValue;
    this.updateValue = updateValue;
  }

  /**
   * A constructor for an operation with {@link EncodedKey}. MessageSenders use the encoded key instead of a raw key,
   * because they reuse the encoded key multiple times (e.g., sending a request that embeds a encoded key,
   * distributing operations evenly to threads, resolving a corresponding block if it's for hashed table).
   * Otherwise serialization happens multiple times.
   * MessageHandlers, on the other hand, do not maintain encoded key
   * (i.e., the other constructor without EncodedKey will be used),
   * because they do not reuse the encoded key (i.e., their reply messages do not include keys).
   *
   * @param origExecutorId an id of the original evaluator where the operation is generated.
   * @param operationId an id of operation
   * @param operationType a type of operation
   * @param replyRequired a boolean representing that the operation requires reply or not
   * @param tableId an id of table
   * @param blockId an id of block
   * @param encodedKey an {@link EncodedKey} of a data key
   * @param dataValue a value of data. It is null when the operation is one of GET or REMOVE.
   * @param updateValue a value to be used in {@link edu.snu.cay.services.et.evaluator.api.UpdateFunction},
   *                    which is not null only when UPDATE.
   */
  DataOpMetadata(final String origExecutorId,
                 final long operationId, final OpType operationType,
                 final boolean replyRequired,
                 final String tableId, final int blockId,
                 final EncodedKey<K> encodedKey,
                 @Nullable final V dataValue,
                 @Nullable final U updateValue) {
    this.origExecutorId = origExecutorId;
    this.operationId = operationId;
    this.operationType = operationType;
    this.replyRequired = replyRequired;
    this.tableId = tableId;
    this.blockId = blockId;
    this.dataKey = encodedKey.getKey();
    this.encodedKey = encodedKey;
    this.dataValue = dataValue;
    this.updateValue = updateValue;
  }

  /**
   * @return an id of executor that has issued the operation originally
   */
  String getOrigId() {
    return origExecutorId;
  }

  /**
   * @return an id of operation, which is unique within an executor that is an origin of the operation
   */
  long getOpId() {
    return operationId;
  }

  /**
   * @return a type of operation (e.g., GET, PUT, UPDATE, REMOVE)
   */
  OpType getOpType() {
    return operationType;
  }

  boolean isReplyRequired() {
    return replyRequired;
  }

  /**
   * @return an id of a table
   */
  String getTableId() {
    return tableId;
  }

  /**
   * @return an id of a block to which a key belongs
   */
  int getBlockId() {
    return blockId;
  }

  /**
   * @return a key of data
   */
  K getKey() {
    return dataKey;
  }

  /**
   * @return an {@link Optional} with an {@link EncodedKey} of data key.
   */
  Optional<EncodedKey<K>> getEncodedKey() {
    return Optional.ofNullable(encodedKey);
  }

  /**
   * @return an Optional with value of data. It's empty when the type of operation is not PUT or PUT_IF_ABSENT
   */
  Optional<V> getValue() {
    return Optional.ofNullable(dataValue);
  }

  /**
   * @return an Optional with update value. It's empty when the type of operation is not UPDATE
   */
  Optional<U> getUpdateValue() {
    return Optional.ofNullable(updateValue);
  }
}
