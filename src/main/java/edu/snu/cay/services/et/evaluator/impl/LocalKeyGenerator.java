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

import edu.snu.cay.services.et.exceptions.KeyGenerationException;
import org.apache.commons.lang3.tuple.Pair;

import javax.inject.Inject;
import java.util.*;

/**
 * A class that generates keys for local blocks. It works only for ordered tables whose key type is {@link Long}.
 * Note that the result of {@link #getKeys(int)} can partially be wrong,
 * because blocks can move to other executors after return.
 */
final class LocalKeyGenerator {
  private final OwnershipCache ownershipCache;
  private final OrderingBasedBlockPartitioner orderingBasedBlockPartitioner;

  @Inject
  private LocalKeyGenerator(final OwnershipCache ownershipCache,
                            final OrderingBasedBlockPartitioner orderingBasedBlockPartitioner) {
    this.ownershipCache = ownershipCache;
    this.orderingBasedBlockPartitioner = orderingBasedBlockPartitioner;
  }

  /**
   * Returns a list of keys as many as {@code numKeys} that belong to local blocks.
   * It distributes keys to blocks as even as possible.
   * Note that this method is stateless, so calling multiple times gives the same result
   * if the entry of local blocks is not changed.
   * @param numKeys the number of keys
   * @return a list of keys that belong to local blocks
   * @throws KeyGenerationException when the number of keys in one block has exceeded its limit
   */
  List<Long> getKeys(final int numKeys) throws KeyGenerationException {
    // obtain blockToKeySpace info
    final Map<Integer, Pair<Long, Long>> blockToKeySpace = new HashMap<>();
    final List<Integer> localBlockIds = ownershipCache.getCurrentLocalBlockIds();
    localBlockIds.forEach(blockId ->
        blockToKeySpace.put(blockId, orderingBasedBlockPartitioner.getKeySpace(blockId)));

    final int numBlocks = blockToKeySpace.size();

    // actual number of keys per block is 'numKeysPerBlock' or 'numKeysPerBlock + 1'. (+1 is for handling remainders)
    final int numKeysPerBlock = numKeys / numBlocks;
    int numRemainingKeys = numKeys % numBlocks; // remainder after dividing equal amount of keys to all blocks

    final List<Long> keys = new ArrayList<>(numKeys);

    for (final int blockId : blockToKeySpace.keySet()) {
      final Pair<Long, Long> keySpace = blockToKeySpace.get(blockId);
      final long maxKey = keySpace.getRight();
      final long minKey = keySpace.getLeft();

      final int numKeysToAllocate;
      if (numRemainingKeys > 0) {
        numRemainingKeys--;
        numKeysToAllocate = numKeysPerBlock + 1;
      } else {
        numKeysToAllocate = numKeysPerBlock;
      }

      // though we may delegate an overflow to other blocks that have space, let's just throw exception
      if (maxKey - minKey + 1 < numKeysToAllocate) {
        throw new KeyGenerationException("The number of keys in one block has exceeded its limit.");
      }

      // simply start from minKey
      for (long key = minKey; key < minKey + numKeysToAllocate; key++) {
        keys.add(key);
      }
    }

    return keys;
  }
}
