/*
 * Copyright (C) 2015 Seoul National University
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

import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import org.apache.commons.lang.math.LongRange;

import java.util.Collection;

/**
 * Encapsulates an update of the MemoryStore's state.
 * The data is added or removed when apply() is called.
 */
interface Update {
  /**
   * Type of the Update.
   */
  enum Type {
    ADD, REMOVE
  }

  /**
   * @return Type of the Update.
   */
  Type getType();

  /**
   * @return Ranges that will be affected by this update.
   */
  Collection<LongRange> getRanges();

  /**
   * Apply the changes to the MemoryStore.
   * @param memoryStore MemoryStore to add/remove the data.
   */
  void apply(MemoryStore memoryStore);
}
