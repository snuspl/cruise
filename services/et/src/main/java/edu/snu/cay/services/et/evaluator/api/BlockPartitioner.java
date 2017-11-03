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
package edu.snu.cay.services.et.evaluator.api;

import edu.snu.cay.services.et.evaluator.impl.EncodedKey;

/**
 * A class that partitions keys into blocks.
 */
public interface BlockPartitioner<K> {

  /**
   * Get an id of block which contains a given key.
   * @param key a key
   * @return a block id
   */
  int getBlockId(K key);

  /**
   * Get an id of block which contains a key of a given {@link EncodedKey}.
   * @param encodedKey an encoded key
   * @return a block id
   */
  int getBlockId(EncodedKey<K> encodedKey);
}
