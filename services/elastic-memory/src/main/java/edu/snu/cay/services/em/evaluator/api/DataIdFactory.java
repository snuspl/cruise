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
package edu.snu.cay.services.em.evaluator.api;

import edu.snu.cay.services.em.evaluator.impl.IdGenerationException;

import java.util.List;

/**
 * Interface to create globally unique ids when storing data in {@code MemoryStore}.
 * {@code DataIdFactory} should create ids without conflict between each other.
 * It is recommended to create unique ids without asking each other whether this id is used or not,
 * to avoid tedious communications between evaluators.
 * @param <T> type of data id
 */
public interface DataIdFactory<T> {
  /**
   * Give a new data id.
   * Throws {@code RuntimeException} when it is impossible to create unique id.
   * @return new data id
   */
  T getId() throws IdGenerationException;

  /**
   * Give new data ids.
   * Throws {@code RuntimeException} when it is impossible to create unique ids.
   * @param size number of data ids to return
   * @return List of new data ids
   */
  List<T> getIds(int size) throws IdGenerationException;
}
