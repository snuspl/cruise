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

import edu.snu.cay.services.em.evaluator.api.SubMemoryStore;
import org.apache.commons.lang.NotImplementedException;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.io.network.util.Pair;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A {@code SubMemoryStore} implementation based on {@code TreeMap}s inside a single {@code HashMap}.
 * All data of one data type is stored in a {@code TreeMap}, ordered by data ids.
 * These {@code TreeMap}s are then maintained as values of one big {@code HashMap}, which uses the data types as keys.
 * A {@code ReentrantReadWriteLock} is used for synchronization between {@code get}, {@code put},
 * and {@code remove} operations.
 */
@EvaluatorSide
public final class SubMemoryStoreImpl implements SubMemoryStore {

  /**
   * This map uses data types, represented as strings, for keys and inner {@code TreeMaps} for values.
   * Each inner {@code TreeMap} serves as a collection of data of the same data type.
   * {@code TreeMap}s are used for guaranteeing log(n) read and write operations, especially
   * {@code getRange()} and {@code removeRange()} which are ranged queries based on the ids.
   */
  private final Map<String, TreeMap<Long, Object>> dataMap;

  /**
   * Used for synchronization between operations.
   * {@code get} uses the read lock, while {@code put} and {@code remove} use the write lock.
   */
  private final ReadWriteLock readWriteLock;

  @Inject
  SubMemoryStoreImpl() {
    dataMap = new HashMap<>();
    readWriteLock = new ReentrantReadWriteLock(true);
  }

  @Override
  public <T> void put(final String dataType, final long id, final T value) {
    readWriteLock.writeLock().lock();

    try {
      if (!dataMap.containsKey(dataType)) {
        dataMap.put(dataType, new TreeMap<Long, Object>());
      }
      dataMap.get(dataType).put(id, value);

    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  @Override
  public <T> void putList(final String dataType, final List<Long> ids, final List<T> values) {
    if (ids.size() != values.size()) {
      throw new RuntimeException("Different list sizes: ids " + ids.size() + ", values " + values.size());
    }

    readWriteLock.writeLock().lock();

    try {
      if (!dataMap.containsKey(dataType)) {
        dataMap.put(dataType, new TreeMap<Long, Object>());
      }
      final Map<Long, Object> innerMap = dataMap.get(dataType);
      for (int index = 0; index < ids.size(); index++) {
        innerMap.put(ids.get(index), values.get(index));
      }

    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  @Override
  public <T> Pair<Long, T> get(final String dataType, final long id) {
    readWriteLock.readLock().lock();

    try {
      if (!dataMap.containsKey(dataType)) {
        return null;
      }

      final T retObj = (T) dataMap.get(dataType).get(id);
      if (retObj == null) {
        return null;
      }

      return new Pair<>(id, retObj);

    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  @Override
  public <T> Map<Long, T> getAll(final String dataType) {
    readWriteLock.readLock().lock();

    try {
      if (!dataMap.containsKey(dataType)) {
        return null;
      }
      return (Map<Long, T>)dataMap.get(dataType).clone();

    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  @Override
  public <T> Map<Long, T> getRange(final String dataType, final long startId, final long endId) {
    readWriteLock.readLock().lock();

    try {
      if (!dataMap.containsKey(dataType)) {
        return null;
      }
      return new TreeMap<>((SortedMap<Long, T>)dataMap.get(dataType).subMap(startId, true, endId, true));

    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  @Override
  public <T> Pair<Long, T> remove(final String dataType, final long id) {
    readWriteLock.writeLock().lock();

    try {
      if (!dataMap.containsKey(dataType)) {
        return null;
      }

      final T retObj = (T) dataMap.get(dataType).remove(id);
      if (retObj == null) {
        return null;
      }

      return new Pair<>(id, retObj);

    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  @Override
  public <T> Map<Long, T> removeAll(final String dataType) {
    readWriteLock.writeLock().lock();

    try {
      if (!dataMap.containsKey(dataType)) {
        return null;
      }
      return (Map<Long, T>)dataMap.remove(dataType);

    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  @Override
  public <T> Map<Long, T> removeRange(final String dataType, final long startId, final long endId) {
    readWriteLock.writeLock().lock();

    try {
      if (!dataMap.containsKey(dataType)) {
        return null;
      }
      final TreeMap<Long, T> subMap =
          new TreeMap<>((SortedMap<Long, T>)dataMap.get(dataType).subMap(startId, true, endId, true));
      dataMap.get(dataType).keySet().removeAll(subMap.keySet());
      return subMap;

    } finally {
      readWriteLock.writeLock().unlock();
    }
  }
}
