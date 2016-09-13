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
 * Interface for notifying block update(put/remove) events to clients from the MemoryStore.
 * The MemoryStore will call the onNext method in this class which clients have defined.
 */
public interface BlockUpdateNotifyListener<K> {
  /**
   * a callback function called at each event of block addition to the Memory Store.
   * @param blockId the id of an added block
   * @param addedKeys a set of keys added at the block addition event
   */
  void onAddedBlock(int blockId, Set<K> addedKeys);

  /**
   * a callback function called at each event of block removal from Memory Store.
   * @param blockId the id of a removed block
   * @param removedKeys a set of keys removed at the block removal event
   */
  void onRemovedBlock(int blockId, Set<K> removedKeys);

}
