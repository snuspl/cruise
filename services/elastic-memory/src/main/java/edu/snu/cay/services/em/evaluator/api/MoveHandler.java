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
package edu.snu.cay.services.em.evaluator.api;

import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.annotations.audience.Private;

import java.util.Map;

/**
 * Interface for updating the ownership and getting/putting data from/to the MemoryStore.
 * Methods in this class manage the ownership and data in the MemoryStore in block-level,
 * contrary to public APIs where the data is accessed in key-level.
 *
 */
@EvaluatorSide
@Private
interface MoveHandler<K> {
  /**
   * Called when the ownership arrives, to apply the change of ownership.
   * @param blockId id of the block to update its owner
   * @param oldOwnerId id of the MemoryStore who was the owner
   * @param newOwnerId id of the MemoryStore who will be the owner
   */
  void updateOwnership(int blockId, int oldOwnerId, int newOwnerId);

  /**
   * Sends the data in the blocks to another MemoryStore.
   * @param blockId the identifier of block to send
   * @param data the data to put
   */
  void putBlock(int blockId, Map<K, Object> data);

  /**
   * Gets the data in the block.
   * @param blockId id of the block to get
   * @return the data in the requested block.
   */
  Map<K, Object> getBlock(int blockId);

  /**
   * Removes the data from the MemoryStore.
   * @param blockId id of the block to remove
   */
  void removeBlock(int blockId);
}
