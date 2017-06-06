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

/**
 * An abstract class of metadata which are stored in {@link RemoteDataOp}.
 * Depending on the number of keys and values, {@link SingleKeyDataOpMetadata} or
 * {@link MultiKeyDataOpMetadata} inherits this class.
 */
abstract class DataOpMetadata {

  /**
   * A common metadata of the operation for both single-key and multi-key version.
   */
  private final String origExecutorId;
  private final long operationId;
  private final OpType operationType;
  private final boolean replyRequired;
  private final String tableId;
  private final int blockId;

  /**
   * A boolean to represent this operation is single-key version or multi-key version.
   * Users can cast an object  figure out which concrete class
   */
  private final boolean singleKey;

  /**
   * A constructor for an operation.
   * @param origExecutorId an id of the original evaluator where the operation is generated.
   * @param operationId an id of operation
   * @param operationType a type of operation
   * @param replyRequired a boolean representing that the operation requires reply or not
   * @param singleKey a boolean representing that the operation is for single key or multi-key
   * @param tableId an id of table
   * @param blockId an id of block
   */
  DataOpMetadata(final String origExecutorId,
                 final long operationId, final OpType operationType,
                 final boolean replyRequired,
                 final boolean singleKey,
                 final String tableId, final int blockId) {
    this.origExecutorId = origExecutorId;
    this.operationId = operationId;
    this.operationType = operationType;
    this.replyRequired = replyRequired;
    this.singleKey = singleKey;
    this.tableId = tableId;
    this.blockId = blockId;
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

  /**
   * @return true if it requires reply
   */
  boolean isReplyRequired() {
    return replyRequired;
  }

  /**
   * @return true if it is for single-key, not multi-key
   */
  boolean isSingleKey() {
    return singleKey;
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

}
