package edu.snu.cay.services.em.evaluator.impl;

import edu.snu.cay.services.em.evaluator.api.SubMemoryStore;
import org.apache.commons.lang.NotImplementedException;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.io.network.util.Pair;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@EvaluatorSide
public final class SubMemoryStoreImpl implements SubMemoryStore {

  private final Map<String, TreeMap<Long, Object>> dataMap;
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
      for (int index = 0; index < ids.size(); index++) {
        dataMap.get(dataType).put(ids.get(index), values.get(index));
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

  @Override
  public boolean hasChanged() {
    throw new NotImplementedException();
  }
}
