/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.cay.services.em.driver.api;

import org.apache.reef.annotations.audience.Private;

import java.util.Map;

/**
 * Holds the routing table.
 */
@Private
public final class RoutingInfo {
  private final Map<Integer, Integer> blockIdToStoreId;
  private final String evalPrefix;
  private final long blockSize;

  public RoutingInfo(final Map<Integer, Integer> blockIdToStoreId,
                     final String evalPrefix,
                     final long blockSize) {
    this.blockIdToStoreId = blockIdToStoreId;
    this.evalPrefix = evalPrefix;
    this.blockSize = blockSize;
  }

  /**
   * @return The mapping between block ids and memory store ids.
   */
  public Map<Integer, Integer> getBlockIdToStoreId() {
    return blockIdToStoreId;
  }

  /**
   * @return The size of one block in MemoryStore.
   */
  public long getBlockSize() {
    return blockSize;
  }

  /**
   * @return The prefix that is used to convert memory store id to evaluator id.
   */
  public String getEvalPrefix() {

    return evalPrefix;
  }
}
