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
package edu.snu.cay.services.em.evaluator.impl.rangekey;

import edu.snu.cay.services.em.evaluator.api.BlockResolver;
import org.apache.reef.io.network.util.Pair;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * A class for splitting a key range into sub ranges cut by block boundary.
 */
final class RangeSplitter<K> {

  private final BlockResolver<K> blockResolver;

  @Inject
  private RangeSplitter(final BlockResolver<K> blockResolver) {
    this.blockResolver = blockResolver;
  }

  /**
   * Splits a key range into multiple sub ranges cut by block boundary.
   * Multiple sub ranges may exist for a single block.
   * @param dataKeyRanges a key range
   * @return a map between a block id and a list of corresponding sub ranges
   */
  Map<Integer, List<Pair<K, K>>> splitIntoSubKeyRanges(final List<Pair<K, K>> dataKeyRanges) {
    // split into ranges per block
    final Map<Integer, List<Pair<K, K>>> blockToSubKeyRangesMap = new HashMap<>();

    dataKeyRanges.forEach(keyRange -> {
      final Map<Integer, Pair<K, K>> blockToSubKeyRangeMap =
          blockResolver.resolveBlocksForOrderedKeys(keyRange.getFirst(), keyRange.getSecond());

      blockToSubKeyRangeMap.forEach((blockId, subKeyRange) ->
          blockToSubKeyRangesMap.computeIfAbsent(blockId, integer -> new LinkedList<>()).add(subKeyRange));
    });

    return blockToSubKeyRangesMap;
  }
}
