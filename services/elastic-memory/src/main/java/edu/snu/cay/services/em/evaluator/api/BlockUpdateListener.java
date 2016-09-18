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

import java.util.Set;


/**
 * Interface for notifying the result of data migration occurred in the MemoryStore
 * according to the event type (addition / removal of blocks).
 * In the implementation, Clients specify how they would like to react to the data migration.
 */
public interface BlockUpdateListener<K> {

  /**
   * A callback function called at each event of block addition to the {@link MemoryStore}.
   * @param blockId the id of an added block
   * @param addedKeys a set of keys added at the block addition event
   */
  void onAddedBlock(int blockId, Set<K> addedKeys);

  /**
   * A callback function called at each event of block removal from {@link MemoryStore}.
   * @param blockId the id of a removed block
   * @param removedKeys a set of keys removed at the block removal event
   */
  void onRemovedBlock(int blockId, Set<K> removedKeys);
}
