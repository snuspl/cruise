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
 * A single version of {@link DataOpMetadata}.
 */
class SingleKeyDataOpMetadata<K, V, U> extends DataOpMetadata {

  /**
   * Metadata of the operation for single version.
   */
  private final K dataKey;
  private final V dataValue;
  private final U updateValue;

  SingleKeyDataOpMetadata(final String origExecutorId,
                          final long operationId, final OpType operationType,
                          final boolean replyRequired,
                          final String tableId, final int blockId,
                          final K dataKey,
                          @Nullable final V dataValue,
                          @Nullable final U updateValue) {
    super(origExecutorId, operationId, operationType, replyRequired, true, tableId, blockId);
    this.dataKey = dataKey;
    this.dataValue = dataValue;
    this.updateValue = updateValue;
  }

  /**
   * @return a key of data
   */
  K getKey() {
    return dataKey;
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
