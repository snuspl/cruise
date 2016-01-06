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
import org.apache.reef.io.network.util.Pair;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
  public <T> void put(final String dataType, final long id, final T value) {
    elasticStore.put(dataType, id, value);
  }

  @Override
  public <T> void putList(final String dataType, final List<Long> ids, final List<T> values) {
    elasticStore.putList(dataType, ids, values);
  }

  @Override
  public <T> Pair<Long, T> get(final String dataType, final long id) {
    return elasticStore.get(dataType, id);
  }

  @Override
  public <T> Map<Long, T> getAll(final String dataType) {
    return elasticStore.getAll(dataType);
  }

  @Override
  public <T> Map<Long, T> getRange(final String dataType, final long startId, final long endId) {
    return elasticStore.getRange(dataType, startId, endId);
  }

  @Override
  public <T> Pair<Long, T> remove(final String dataType, final long id) {
    return elasticStore.remove(dataType, id);
  }

  @Override
  public <T> Map<Long, T> removeAll(final String dataType) {
    return elasticStore.removeAll(dataType);
  }

  @Override
  public <T> Map<Long, T> removeRange(final String dataType, final long startId, final long endId) {
    return elasticStore.removeRange(dataType, startId, endId);
  }

  @Override
  public Set<String> getDataTypes() {
    return elasticStore.getDataTypes();
  }

  @Override
  public int getNumUnits(final String dataType) {
    return elasticStore.getNumUnits(dataType);
  }
}
