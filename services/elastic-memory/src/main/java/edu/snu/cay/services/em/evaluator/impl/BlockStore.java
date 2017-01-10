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
package edu.snu.cay.services.em.evaluator.impl;

import edu.snu.cay.services.em.evaluator.api.Block;
import edu.snu.cay.services.em.evaluator.api.BlockFactory;
import edu.snu.cay.services.em.evaluator.api.BlockHandler;
import edu.snu.cay.services.em.evaluator.api.BlockUpdateListener;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.annotations.audience.Private;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A store that contains all local blocks.
 */
@Private
@ThreadSafe
@EvaluatorSide
public final class BlockStore<K, V> implements BlockHandler<K, V> {
  /**
   * Maintains blocks associated with blockIds.
   */
  private final ConcurrentMap<Integer, Block<K, V>> blocks = new ConcurrentHashMap<>();

  /**
   * Block update listeners that clients have registered.
   */
  private final Set<BlockUpdateListener<K>> blockUpdateListeners
      = Collections.newSetFromMap(new ConcurrentHashMap<>());

  private final OperationRouter router;
  private final BlockFactory blockFactory;

  @Inject
  private BlockStore(final OperationRouter router,
                     final BlockFactory blockFactory) {
    this.router = router;
    this.blockFactory = blockFactory;
    initBlocks();
  }

  /**
   * Initialize local blocks.
   */
  private void initBlocks() {
    for (final int blockId : router.getInitialLocalBlockIds()) {
      blocks.put(blockId, blockFactory.newBlock());
    }
  }

  public Block get(final int blockId) {
    return blocks.get(blockId);
  }

  public int getNumBlocks() {
    return blocks.size();
  }

  @Override
  public void putBlock(final int blockId, final Map<K, V> data) {
    final Block<K, V> block = blockFactory.newBlock();
    block.putAll(data);

    if (blocks.putIfAbsent(blockId, block) != null) {
      throw new RuntimeException("Block with id " + blockId + " already exists.");
    }
    notifyBlockAddition(blockId, block);
  }

  @Override
  public Map<K, V> getBlock(final int blockId) {
    final Block<K, V> block = blocks.get(blockId);
    if (null == block) {
      throw new RuntimeException("Block with id " + blockId + "does not exist.");
    }

    return block.getAll();
  }

  @Override
  public void removeBlock(final int blockId) {
    final Block block = blocks.remove(blockId);
    if (null == block) {
      throw new RuntimeException("Block with id " + blockId + "does not exist.");
    }
    notifyBlockRemoval(blockId, block);
  }

  public boolean registerBlockUpdateListener(final BlockUpdateListener<K> listener) {
    return blockUpdateListeners.add(listener);
  }

  private void notifyBlockRemoval(final int blockId, final Block block) {
    final Map<K, V> kvData = block.getAll();
    final Set<K> keySet = kvData.keySet();
    for (final BlockUpdateListener<K> listener : blockUpdateListeners) {
      listener.onRemovedBlock(blockId, keySet);
    }
  }

  private void notifyBlockAddition(final int blockId, final Block block) {
    final Map<K, V> kvData = block.getAll();
    final Set<K> keySet = kvData.keySet();
    for (final BlockUpdateListener<K> listener : blockUpdateListeners) {
      listener.onAddedBlock(blockId, keySet);
    }
  }
}
