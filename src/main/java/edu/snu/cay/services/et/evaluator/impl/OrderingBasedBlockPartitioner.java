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

import edu.snu.cay.services.et.configuration.parameters.NumTotalBlocks;
import edu.snu.cay.services.et.evaluator.api.BlockPartitioner;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * It partitions key space into disjoint ranges as many as the number of blocks
 * and assigns the range to each block.
 * It works only for keys with {@code Long} type.
 */
public final class OrderingBasedBlockPartitioner implements BlockPartitioner<Long> {

  /**
   * A class that partitions key space into disjoint ranges that have same length.
   */
  private final KeySpacePartitioner keySpacePartitioner;

  @Inject
  private OrderingBasedBlockPartitioner(@Parameter(NumTotalBlocks.class) final int numTotalBlocks) {
    this.keySpacePartitioner = new KeySpacePartitioner(0, Long.MAX_VALUE, numTotalBlocks);
  }

  @Override
  public int getBlockId(final Long key) {
    return keySpacePartitioner.getPartitionId(key);
  }

  /**
   * @param blockId a range id
   * @return a pair of min and max key, a keyspace of a block whose id is blockId.
   */
  Pair<Long, Long> getKeySpace(final int blockId) {
    return keySpacePartitioner.getKeySpace(blockId);
  }
}
