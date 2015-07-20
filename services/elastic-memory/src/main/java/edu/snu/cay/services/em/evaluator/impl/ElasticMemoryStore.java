package edu.snu.cay.services.em.evaluator.impl;

import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import edu.snu.cay.services.em.trace.HTrace;
import org.apache.commons.lang.NotImplementedException;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.io.network.util.Pair;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * MemoryStore class that allows its data to be migrated around evaluators.
 * Backed by a data map for local data and another one for non-local data.
 * Local data is assumed to have no identifiers, while non-local data is assumed to always have identifiers.
 * As a result, methods with identifier parameters use only the non-local data map.
 */
@EvaluatorSide
public final class ElasticMemoryStore implements MemoryStore {

  private final Map<String, List> localDataMap;
  private final Map<String, TreeMap<Long, Object>> elasticDataMap;
  private final ReadWriteLock readWriteLock;

  @Inject
  public ElasticMemoryStore(final HTrace hTrace) {
    hTrace.initialize();
    localDataMap = new HashMap<>();
    elasticDataMap = new HashMap<>();
    readWriteLock = new ReentrantReadWriteLock(true);
  }

  @Override
  public <T> void putLocal(final String key, final T value) {
    readWriteLock.readLock().lock();
    try {
      final List<Object> singleObjectList = new LinkedList<>();
      singleObjectList.add(value);
      localDataMap.put(key, singleObjectList);
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  @Override
  public <T> void putLocal(final String key, final List<T> values) {
    readWriteLock.readLock().lock();
    try {
      localDataMap.put(key, values);
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  @Override
  public <T> void putMovable(final String key, final long id, final T value) {
    readWriteLock.writeLock().lock();
    try {
      if (!elasticDataMap.containsKey(key)) {
        elasticDataMap.put(key, new TreeMap<Long, Object>());
      }
      elasticDataMap.get(key).put(id, value);
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  @Override
  public <T> void putMovable(final String key, final List<Long> ids, final List<T> values) {
    if (ids.size() != values.size()) {
      throw new RuntimeException("Different list sizes: ids " + ids.size() + ", values " + values.size());
    }

    readWriteLock.writeLock().lock();
    try {
      if (!elasticDataMap.containsKey(key)) {
        elasticDataMap.put(key, new TreeMap<Long, Object>());
      }

      for (int index = 0; index < ids.size(); index++) {
        elasticDataMap.get(key).put(ids.get(index), values.get(index));
      }
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> List<T> get(final String key) {
    readWriteLock.readLock().lock();
    try {
      final List<T> retList = new LinkedList<>();
      if (localDataMap.containsKey(key)) {
        retList.addAll((List<T>) localDataMap.get(key));
      }
      if (elasticDataMap.containsKey(key)) {
        retList.addAll((Collection<T>) (elasticDataMap.get(key).values()));
      }
      return retList;
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> Pair<Long, T> get(final String key, final long id) {
    readWriteLock.readLock().lock();
    try {
      if (!elasticDataMap.containsKey(key)) {
        return null;
      }

      return new Pair<>(id, (T) elasticDataMap.get(key).get(id));
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> Pair<Long, T> remove(final String key, final long id) {
    readWriteLock.writeLock().lock();
    try {
      if (!elasticDataMap.containsKey(key)) {
        return null;
      }

      return new Pair<>(id, (T) elasticDataMap.get(key).remove(id));
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> List<Pair<Long, T>> get(final String key, final long startId, final long endId) {
    readWriteLock.readLock().lock();
    try {
      if (!elasticDataMap.containsKey(key)) {
        return null;
      }

      final Map<Long, Object> subMap = elasticDataMap.get(key).subMap(startId, true, endId, true);
      final List<Pair<Long, T>> retList = new LinkedList();
      for (final Map.Entry<Long, Object> entry : subMap.entrySet()) {
        retList.add(new Pair<>(entry.getKey(), (T) entry.getValue()));
      }
      return retList;
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> List<Pair<Long, T>> remove(final String key, final long startId, final long endId) {
    readWriteLock.writeLock().lock();
    try {
      if (!elasticDataMap.containsKey(key)) {
        return null;
      }

      final Map<Long, Object> subMap = elasticDataMap.get(key).subMap(startId, true, endId, true);
      final List<Pair<Long, T>> retList = new LinkedList<>();
      final Set<Long> ids = new HashSet<>();
      for (final Map.Entry<Long, Object> entry : subMap.entrySet()) {
        retList.add(new Pair<>(entry.getKey(), (T) entry.getValue()));
        ids.add(entry.getKey());
      }
      subMap.keySet().removeAll(ids);
      return retList;
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  @Override
  public boolean hasChanged() {
    throw new NotImplementedException();
  }
}
