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
package edu.snu.cay.services.et.evaluator.impl;

import edu.snu.cay.services.et.configuration.parameters.NumTotalBlocks;
import edu.snu.cay.services.et.evaluator.api.PartitionFunction;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * Hash-based implementation of PartitionFunc.
 * @param <K> The type of key. User should provide its {@code hashCode} properly.
 */
public class HashPartitionFunction<K> implements PartitionFunction<K> {
  private final int numTotalBlocks;

  @Inject
  HashPartitionFunction(@Parameter(NumTotalBlocks.class) final int numTotalBlocks) {
    this.numTotalBlocks = numTotalBlocks;
  }

  @Override
  public int getBlockId(final K key) {
    final int hashed = Math.abs(key.hashCode());
    return hashed % numTotalBlocks;
  }
}
