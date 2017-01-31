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

import org.apache.reef.annotations.audience.Private;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A class that stores a tablet comprised of blocks assigned to an executor.
 */
@Private
public final class BlockStore<K, V> {

  /**
   * Maintains blocks associated with blockIds.
   */
  private final ConcurrentMap<Integer, Block> blocks = new ConcurrentHashMap<>();

  @Inject
  private BlockStore() {
  }

  public Block get(final int blockId) {
    return blocks.get(blockId);
  }

  /**
   * Sends the data in the blocks to another MemoryStore.
   *
   * @param blockId the identifier of block to send
   * @param data    the data to put
   */
  void putBlock(final int blockId, final Map<K, V> data) {

  }

  /**
   * Gets the data in the block.
   *
   * @param blockId id of the block to get
   * @return the data in the requested block.
   */
  Map<K, V> getBlock(final int blockId) {
    return null;
  }

  /**
   * Removes the data from the MemoryStore.
   *
   * @param blockId id of the block to remove
   */
  void removeBlock(final int blockId) {

  }

  private final class Block {

  }
}
