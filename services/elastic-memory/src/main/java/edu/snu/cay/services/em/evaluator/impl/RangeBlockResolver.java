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
import org.apache.commons.lang.math.LongRange;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;

/**
 * An implementation of BlockResolver.
 * It groups data keys into blocks with range-based partitioning, where block b takes
 * keys within [b * BLOCK_SIZE, (b+1) * BLOCK_SIZE).
 */
public final class RangeBlockResolver implements BlockResolver {

  private final long blockSize;

  @Inject
  private RangeBlockResolver(@Parameter(NumTotalBlocks.class) final int numTotalBlocks) {
    this.blockSize = Long.MAX_VALUE / numTotalBlocks;
  }

  @Override
  public int resolveBlock(final long dataKey) {
    return (int) (dataKey / blockSize);
  }

  @Override
  public Map<Integer, LongRange> resolveBlocks(final LongRange dataKeyRange) {
    final long headKey = dataKeyRange.getMinimumLong();
    final long tailKey = dataKeyRange.getMaximumLong();

    final int headBlockId = (int) (headKey / blockSize);
    final int tailBlockId = (int) (tailKey / blockSize);

    final Map<Integer, LongRange> blockToKeyRange = new HashMap<>();

    if (headBlockId == tailBlockId) {
      blockToKeyRange.put(headBlockId, dataKeyRange);
    } else {
      blockToKeyRange.put(headBlockId, new LongRange(headKey, (headBlockId + 1) * blockSize - 1));

      for (int blockId = headBlockId + 1; blockId < tailBlockId; blockId++) {
        blockToKeyRange.put(blockId, new LongRange(blockId * blockSize, (blockId + 1) * blockSize - 1));
      }

      blockToKeyRange.put(tailBlockId, new LongRange(tailBlockId * blockSize, tailKey));
    }

    return blockToKeyRange;
  }
}
