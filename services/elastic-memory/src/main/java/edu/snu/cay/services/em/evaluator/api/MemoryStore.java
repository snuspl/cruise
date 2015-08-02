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

import org.apache.reef.annotations.audience.EvaluatorSide;

/**
 * Evaluator-side interface of MemoryStore, which provides two SubMemoryStores: local and elastic.
 */
@EvaluatorSide
public interface MemoryStore {

  /**
   * Returns a {@code SubMemoryStore} which stores local data that
   * should not be moved to other evaluators.
   */
  SubMemoryStore getLocalStore();

  /**
   * Returns a {@code SubMemoryStore} which stores movable data that
   * may be migrated around evaluators for job optimization.
   */
  SubMemoryStore getElasticStore();
}
