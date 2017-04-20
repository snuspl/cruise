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

import edu.snu.cay.services.et.configuration.parameters.KeyCodec;
import edu.snu.cay.services.et.configuration.parameters.NumTotalBlocks;
import edu.snu.cay.services.et.evaluator.api.BlockPartitioner;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * It partitions *hashed* key space into disjoint ranges as many as the number of blocks
 * and assigns the range to each block.
 * @param <K> The type of key. User should provide its {@code hashCode} properly.
 */
public final class HashBasedBlockPartitioner<K> implements BlockPartitioner<K> {
  /**
   * A class that partitions key space into disjoint ranges that have same length.
   */
  private final KeySpacePartitioner keySpacePartitioner;

  private final Codec<K> keyCodec;

  @Inject
  private HashBasedBlockPartitioner(@Parameter(NumTotalBlocks.class) final int numTotalBlocks,
                                    @Parameter(KeyCodec.class) final Codec<K> keyCodec) {
    this.keySpacePartitioner = new KeySpacePartitioner(0, Integer.MAX_VALUE, numTotalBlocks);
    this.keyCodec = keyCodec;
  }

  @Override
  public int getBlockId(final K key) {
    final EncodedKey<K> encodedKey = new EncodedKey<>(key, keyCodec);
    return keySpacePartitioner.getPartitionId(encodedKey.getHash());
  }

  @Override
  public int getBlockId(final EncodedKey<K> encodedKey) {
    return keySpacePartitioner.getPartitionId(encodedKey.getHash());
  }
}
