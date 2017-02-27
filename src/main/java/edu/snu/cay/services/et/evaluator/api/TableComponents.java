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

import edu.snu.cay.services.et.evaluator.impl.BlockStore;
import edu.snu.cay.services.et.evaluator.impl.KVSerializer;
import edu.snu.cay.services.et.evaluator.impl.OwnershipCache;
import edu.snu.cay.services.et.evaluator.impl.TableImpl;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * A private interface for allowing access to internal components of a table.
 */
@DefaultImplementation(TableImpl.class)
@Private
public interface TableComponents<K, V> {

  /**
   * @return a ownership cache of a table
   */
  OwnershipCache getOwnershipCache();

  /**
   * @return a block store of a table
   */
  BlockStore<K, V> getBlockStore();

  /**
   * @return a serializer of a table
   */
  KVSerializer<K, V> getSerializer();

  /**
   * @return a block partitioner of a table
   */
  BlockPartitioner<K> getBlockPartitioner();
}
