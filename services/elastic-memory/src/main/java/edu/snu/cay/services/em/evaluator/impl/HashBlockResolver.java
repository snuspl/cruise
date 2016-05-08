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
import java.util.Map;

/**
 * Resolves the block using the hashed value of the key.
 */
public final class HashBlockResolver<K> implements BlockResolver<K> {
  private final int numTotalBlocks;

  @Inject
  HashBlockResolver(@Parameter(NumTotalBlocks.class) final int numTotalBlocks) {
    this.numTotalBlocks = numTotalBlocks;
  }

  @Override
  public int resolveBlock(final K dataKey) {
    final int hashed = Math.abs(dataKey.hashCode());
    return hashed % numTotalBlocks;
  }

  @Override
  public Map<Integer, Pair<K, K>> resolveBlocksForOrderedKeys(final K minKey, final K maxKey) {
    throw new UnsupportedOperationException();
  }
}
