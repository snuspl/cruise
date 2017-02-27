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

import org.apache.commons.lang3.tuple.Pair;

/**
 * A class that partitions whole key space into the same length of disjoint ranges.
 */
class KeySpacePartitioner {
  private final long partitionSize;

  KeySpacePartitioner(final long minKey,
                      final long maxKey,
                      final int numTotalPartitions) {
    // 1. the scale of partitionSize is always upper integer (32bit)
    this.partitionSize = (maxKey - minKey) / numTotalPartitions;
  }

  /**
   * @param key a key
   * @return a partition id to which the specified key belongs
   */
  int getPartitionId(final long key) {
    // 2. so dividing key by partitionSize always results integer
    return (int) (key / partitionSize);
  }

  /**
   * @param partitionId a partition id
   * @return a pair of min and max key, a keyspace of a partition whose id is partitionId.
   */
  Pair<Long, Long> getKeySpace(final int partitionId) {
    final long minKey = partitionSize * partitionId;
    return Pair.of(minKey, minKey + partitionSize - 1);
  }
}
