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
package edu.snu.cay.services.em.evaluator.impl;

import edu.snu.cay.services.em.common.parameters.NumTotalBlocks;
import edu.snu.cay.services.em.evaluator.api.BlockResolver;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;

/**
 * An implementation of BlockResolver.
 * It groups data keys into blocks with range-based partitioning, where block b takes
 * keys within [b * BLOCK_SIZE, (b+1) * BLOCK_SIZE).
 */
public final class RangeBlockResolver implements BlockResolver<Long> {

  private final long blockSize;

  @Inject
  private RangeBlockResolver(@Parameter(NumTotalBlocks.class) final int numTotalBlocks) {
    this.blockSize = Long.MAX_VALUE / numTotalBlocks;
  }

  @Override
  public int resolveBlock(final Long dataKey) {
    return (int) (dataKey / blockSize);
  }

  @Override
  public Map<Integer, Pair<Long, Long>> resolveBlocksforOrderedKeys(final Long minKey, final Long maxKey) {

    final int headBlockId = (int) (minKey / blockSize);
    final int tailBlockId = (int) (maxKey / blockSize);

    final Map<Integer, Pair<Long, Long>> blockToKeyRange = new HashMap<>();

    if (headBlockId == tailBlockId) {
      // a case when dataKeyRange is in a single block (most common case)
      blockToKeyRange.put(headBlockId, new Pair<>(minKey, maxKey));
    } else {
      // a case when dataKeyRange spans across the multiple blocks
      final long rightEndInHeadBlock = headBlockId * blockSize + blockSize - 1;
      blockToKeyRange.put(headBlockId, new Pair<>(minKey, rightEndInHeadBlock));

      for (int blockId = headBlockId + 1; blockId < tailBlockId; blockId++) {
        final long leftEnd = blockId * blockSize;
        final long rightEnd = leftEnd + blockSize - 1;
        blockToKeyRange.put(blockId, new Pair<>(leftEnd, rightEnd));
      }

      final long leftEndInTailBlock = tailBlockId * blockSize;
      blockToKeyRange.put(tailBlockId, new Pair<>(leftEndInTailBlock, maxKey));
    }

    return blockToKeyRange;
  }
}
