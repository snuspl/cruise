package edu.snu.cay.services.em.evaluator.impl;

import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import org.apache.commons.lang.NotImplementedException;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.util.Optional;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * MemoryStore class that only stores evaluator-local data.
 * Data items may or may not have ids.
 * To support both cases, we use REEF's Optional class:
 * Optional.empty() for items with no ids, and Optional.of() for the rest.
 */
@EvaluatorSide
public final class LocalMemoryStore implements MemoryStore {

  private final Map<String, TreeMap<Optional<Long>, Object>> localDataMap;
  private final ReadWriteLock readWriteLock;

  private Comparator<Optional<Long>> optionalLongComparator = new Comparator<Optional<Long>>() {
    @Override
    public int compare(Optional<Long> o1, Optional<Long> o2) {
      if (!o1.isPresent()) {
        return o2.isPresent() ? 1 : 0;
      } else {
        if (!o2.isPresent()) {
          return -1;
        }

        return (int)(o1.get() - o2.get());
      }
    }
  };

  @Inject
  public LocalMemoryStore() {
    localDataMap = new HashMap<>();
    readWriteLock = new ReentrantReadWriteLock(true);
  }

  @Override
  public <T> void putLocal(final String key, final T value) {
    readWriteLock.writeLock().lock();
    try {
      if (!localDataMap.containsKey(key)) {
        localDataMap.put(key, new TreeMap<>(optionalLongComparator));
      }

      localDataMap.get(key).put(Optional.<Long>empty(), value);
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  @Override
  public <T> void putLocal(final String key, final List<T> values) {
    readWriteLock.writeLock().lock();
    try {
      if (!localDataMap.containsKey(key)) {
        localDataMap.put(key, new TreeMap<>(optionalLongComparator));
      }

      for (final T value : values) {
        localDataMap.get(key).put(Optional.<Long>empty(), value);
      }
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  @Override
  public <T> void putMovable(final String key, final long id, final T value) {
    readWriteLock.writeLock().lock();
    try {
      if (!localDataMap.containsKey(key)) {
        localDataMap.put(key, new TreeMap<>(optionalLongComparator));
      }

      localDataMap.get(key).put(Optional.of(id), value);
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  @Override
  public <T> void putMovable(final String key, final List<Long> ids, final List<T> values) {
    readWriteLock.writeLock().lock();
    try {
      if (ids.size() != values.size()) {
        throw new RuntimeException("Different list sizes: ids " + ids.size() + ", values " + values.size());
      }

      if (!localDataMap.containsKey(key)) {
        localDataMap.put(key, new TreeMap<>(optionalLongComparator));
      }

      for (int index = 0; index < ids.size(); index++) {
        localDataMap.get(key).put(Optional.of(ids.get(index)), values.get(index));
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
      if (!localDataMap.containsKey(key)) {
        return null;
      }
      return new LinkedList<>((Collection<T>) localDataMap.get(key).values());
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> Pair<Long, T> get(final String key, final long id) {
    readWriteLock.readLock().lock();
    try {
      if (!localDataMap.containsKey(key)) {
        return null;
      }

      final T item = (T) localDataMap.get(key).get(Optional.of(id));
      return item != null ? new Pair<>(id, item) : null;
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> Pair<Long, T> remove(final String key, final long id) {
    readWriteLock.writeLock().lock();
    try {
      if (!localDataMap.containsKey(key)) {
        return null;
      }

      final T item = (T) localDataMap.get(key).remove(Optional.of(id));
      return item != null ? new Pair<>(id, item) : null;
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  @Override
  public <T> List<Pair<Long, T>> get(final String key, final long startId, final long endId) {
    readWriteLock.readLock().lock();
    try {
      if (!localDataMap.containsKey(key)) {
        return null;
      }

      final Map<Optional<Long>, Object> subMap =
          localDataMap.get(key).subMap(Optional.of(startId), true, Optional.of(endId), true);
      final List<Pair<Long, T>> retList = new LinkedList();
      for (final Map.Entry<Optional<Long>, Object> entry : subMap.entrySet()) {
        retList.add(new Pair<>(entry.getKey().get(), (T) entry.getValue()));
      }
      return retList;
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  @Override
  public <T> List<Pair<Long, T>> remove(final String key, final long startId, final long endId) {
    readWriteLock.writeLock().lock();
    try {
      if (!localDataMap.containsKey(key)) {
        return null;
      }

      final Map<Optional<Long>, Object> subMap =
          localDataMap.get(key).subMap(Optional.of(startId), true, Optional.of(endId), true);
      final List<Pair<Long, T>> retList = new LinkedList();
      final Set<Optional<Long>> ids = new HashSet<>();
      for (final Map.Entry<Optional<Long>, Object> entry : subMap.entrySet()) {
        retList.add(new Pair<>(entry.getKey().get(), (T) entry.getValue()));
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
