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
 * An interface of parameter set class which block update notification observers receive at block update notification.
 */
public interface BlockUpdateNotifyParameter<K> {
  /**
   * getter for a block update notification type, block put or remove.
   * @return a block update notification type
   */
  int getNotifyType();

  /**
   * getter for the id of the block which is updated.
   * @return a block id
   */
  int getUpdatedBlockId();

  /**
   * getter for the set of the keys in the updated block.
   * @return
   */
  Set<K> getKeySet();
}
