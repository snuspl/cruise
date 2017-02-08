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

import edu.snu.cay.services.et.exceptions.BlockNotFoundException;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.annotations.audience.Private;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Maintains block ownership information for a certain table. This enables faster routing for remote operations.
 * Can handle reordered OwnershipUpdate messages. Revision numbers are attached to OwnershipUpdate messages and
 * it makes best effort to provide up-to-date ownership information for lookup requests.
 */
@EvaluatorSide
@ThreadSafe
@Private
public final class OwnershipCache {
  /**
   * An entry of this map pairs the index of a block and the identifier of the executor that owns the block.
   */
  private final ConcurrentMap<Integer, String> ownershipMap = new ConcurrentHashMap<>();

  @Inject
  private OwnershipCache() {
  }

  /**
   * Initialize this ownership cache.
   * @param blockOwners ownership mapping
   */
  public synchronized void init(final List<String> blockOwners) {
    for (int i = 0; i < blockOwners.size(); i++) {
      ownershipMap.put(i, blockOwners.get(i));
    }
  }

  /**
   * Update block ownership.
   * @param blockId index of the block whose ownership is being changed
   * @param executorId identifier of the new owner of the specified block
   */
  public void update(final int blockId, final String executorId) {
    // Key-set of ownershipMap is never modified once initialized
    if (!ownershipMap.containsKey(blockId)) {
      throw new BlockNotFoundException(blockId);
    }
    ownershipMap.put(blockId, executorId);
  }

  /**
   * Lookup ownership information.
   * @param blockId index of the block
   * @return identifier of the owner of the specified block, or {@code null} if no executor owns the block
   */
  public String lookup(final int blockId) {
    final String executorId = ownershipMap.get(blockId);
    if (executorId == null) {
      throw new BlockNotFoundException(blockId);
    }
    return executorId;
  }
}
