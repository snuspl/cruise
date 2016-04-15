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
package edu.snu.cay.services.em.evaluator.api;

import edu.snu.cay.services.em.evaluator.impl.RangeBlockResolver;
import org.apache.commons.lang.math.LongRange;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.Map;

/**
 * An interface for store to resolve the block of specific data key.
 */
@DefaultImplementation(RangeBlockResolver.class)
public interface BlockResolver {

  /**
   * Return a block id of data with {@code dataId}.
   * The number of blocks is assumed to be within the integer range.
   * @param dataKey a key of data
   * @return an id of block that the data belongs to
   */
  int resolveBlock(long dataKey);

  /**
   * Return block ids for a range of data keys, which may span over multiple blocks.
   * Each block contains a single sub key range.
   * @param dataKeyRange a range of data keys
   * @return a map between a block id and a range of data keys
   */
  Map<Integer, LongRange> resolveBlocks(LongRange dataKeyRange);
}
