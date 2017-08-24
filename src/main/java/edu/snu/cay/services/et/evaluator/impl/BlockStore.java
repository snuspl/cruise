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

import edu.snu.cay.services.et.evaluator.api.Block;
import edu.snu.cay.services.et.evaluator.api.UpdateFunction;
import edu.snu.cay.services.et.exceptions.BlockAlreadyExistsException;
import edu.snu.cay.services.et.exceptions.BlockNotExistsException;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.annotations.audience.Private;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A class that stores blocks assigned to an executor.
 */
@EvaluatorSide
@ThreadSafe
@Private
public final class BlockStore<K, V, U> implements Iterable<Block<K, V, U>> {
  private final UpdateFunction<K, V, U> updateFunction;

  /**
   * A mapping with indices and corresponding blocks.
   */
  private final ConcurrentMap<Integer, Block<K, V, U>> blocks = new ConcurrentHashMap<>();

  @Inject
  private BlockStore(final UpdateFunction<K, V, U> updateFunction) {
    this.updateFunction = updateFunction;
  }

  /**
   * Create an empty block.
   * @param blockId index of the block
   * @throws BlockAlreadyExistsException when the specified block already exists
   */
  public void createEmptyBlock(final int blockId) throws BlockAlreadyExistsException {
    if (blocks.putIfAbsent(blockId, new BlockImpl<>(blockId, updateFunction)) != null) {
      throw new BlockAlreadyExistsException(blockId);
    }
  }

  /**
   * Import a block into BlockStore.
   * @param blockId index of the block
   * @param data content of the block
   * @throws BlockAlreadyExistsException when the specified block already exists
   */
  public void putBlock(final int blockId, final Map<K, V> data) throws BlockAlreadyExistsException {
    final Block<K, V, U> block = new BlockImpl<>(blockId, updateFunction);
    block.putAll(data);
    if (blocks.putIfAbsent(blockId, block) != null) {
      throw new BlockAlreadyExistsException(blockId);
    }
  }

  /**
   * Remove a block from BlockStore.
   * @param blockId index of the block
   * @throws BlockNotExistsException when the specified block does not exist
   */
  public void removeBlock(final int blockId) throws BlockNotExistsException {
    final Block<K, V, U> block = blocks.remove(blockId);
    if (block == null) {
      throw new BlockNotExistsException(blockId);
    }
    block.clear(); // in order to reflect change to localDataIterator that iterates on this block
  }

  /**
   * Return contents of the specified block.
   * @param blockId index of the block
   * @return contents of the specified block
   * @throws BlockNotExistsException when the block does not exist
   */
  public Map<K, V> getBlock(final int blockId) throws BlockNotExistsException {
    return get(blockId).getAll();
  }

  /**
   * @return the number of blocks in the store.
   */
  int getNumBlocks() {
    return blocks.size();
  }

  /**
   * Return the specified block.
   * @param blockId index of the block
   * @return block with the specified index
   * @throws BlockNotExistsException when the block does not exist
   */
  public Block<K, V, U> get(final int blockId) throws BlockNotExistsException {
    final Block<K, V, U> block = blocks.get(blockId);
    if (block == null) {
      throw new BlockNotExistsException(blockId);
    }
    return block;
  }

  @Override
  public Iterator<Block<K, V, U>> iterator() {
    return blocks.values().iterator();
  }
}
