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
import edu.snu.cay.services.em.evaluator.api.SubMemoryStore;
import edu.snu.cay.services.em.trace.HTrace;
import org.apache.reef.annotations.audience.EvaluatorSide;

import javax.inject.Inject;

/**
 * Provides local and elastic {@code SubMemoryStore}s that use the same implementation, {@code SubMemoryStoreImpl}.
 * Assuming EM applications always need to instantiate this class, HTrace initialization is done in the constructor.
 */
@EvaluatorSide
public final class MemoryStoreImpl implements MemoryStore {

  /**
   * In-memory storage for data that needs to be maintained in this particular evaluator.
   * This store does not necessarily prevent remote access from other evaluators.
   * For example, this store's data could be replicated elsewhere.
   * Nonetheless, data of this store will not be removed by the system.
   */
  private final SubMemoryStore localStore;

  /**
   * In-memory storage for data that can moved to other evaluators by the system.
   * The store can grow or shrink, depending on the status of this evaluator.
   */
  private final SubMemoryStore elasticStore;

  @Inject
  private MemoryStoreImpl(final HTrace hTrace) {
    hTrace.initialize();
    this.localStore = new SubMemoryStoreImpl();
    this.elasticStore = new SubMemoryStoreImpl();
  }

  @Override
  public SubMemoryStore getLocalStore() {
    return this.localStore;
  }

  @Override
  public SubMemoryStore getElasticStore() {
    return this.elasticStore;
  }
}
