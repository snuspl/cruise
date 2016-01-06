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
import edu.snu.cay.utils.trace.HTrace;
import org.apache.reef.annotations.audience.EvaluatorSide;

import javax.inject.Inject;

/**
 * Provides elastic {@code SubMemoryStore} that use {@code SubMemoryStoreImpl} implementation.
 * Assuming EM applications always need to instantiate this class, HTrace initialization is done in the constructor.
 */
@EvaluatorSide
public final class MemoryStoreImpl implements MemoryStore {

  /**
   * In-memory storage for data that can moved to other evaluators by the system.
   * The store can grow or shrink, depending on the status of this evaluator.
   */
  private final SubMemoryStore elasticStore;

  @Inject
  private MemoryStoreImpl(final HTrace hTrace) {
    hTrace.initialize();
    this.elasticStore = new SubMemoryStoreImpl();
  }

  @Override
  public SubMemoryStore getElasticStore() {
    return this.elasticStore;
  }
}
