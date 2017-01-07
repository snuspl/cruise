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

import edu.snu.cay.services.em.evaluator.impl.BlockStore;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.Map;

/**
 * Interface for getting/putting block data.
 */
@EvaluatorSide
@Private
@DefaultImplementation(BlockStore.class)
public interface BlockHandler<K, V> {
  /**
   * Sends the data in the blocks to another MemoryStore.
   * @param blockId the identifier of block to send
   * @param data the data to put
   */
  void putBlock(int blockId, Map<K, V> data);

  /**
   * Gets the data in the block.
   * @param blockId id of the block to get
   * @return the data in the requested block.
   */
  Map<K, V> getBlock(int blockId);

  /**
   * Removes the data from the MemoryStore.
   * @param blockId id of the block to remove
   */
  void removeBlock(int blockId);
}
