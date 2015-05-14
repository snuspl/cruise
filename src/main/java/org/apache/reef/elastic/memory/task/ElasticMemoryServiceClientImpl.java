package org.apache.reef.elastic.memory.task;

import org.apache.reef.elastic.memory.Key;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;

public class ElasticMemoryServiceClientImpl implements ElasticMemoryServiceClient {

  private final MemoryStorage localMemoryStorage;
  private final MemoryStorage elasticMemoryStorage;

  @Inject
  public ElasticMemoryServiceClientImpl() {
    localMemoryStorage = new MemoryStorage();
    elasticMemoryStorage = new MemoryStorage();
  }

  @Override
  public <T> void putLocal(Class<? extends Key<T>> key, T value) {
    localMemoryStorage.put(key, value);
  }

  @Override
  public <T> void putLocal(Class<? extends Key<T>> key, List<T> values) {
    localMemoryStorage.put(key, values);
  }

  @Override
  public <T> void putMovable(Class<? extends Key<T>> key, T value) {
    elasticMemoryStorage.put(key, value);
  }

  @Override
  public <T> void putMovable(Class<? extends Key<T>> key, List<T> values) {
    elasticMemoryStorage.put(key, values);
  }

  @Override
  public <T> List<T> get(Class<? extends Key<T>> key) {
    final List<T> retList = new LinkedList<>(localMemoryStorage.get(key));
    retList.addAll(elasticMemoryStorage.get(key));
    return retList;
  }

  @Override
  public boolean hasChanged() {
    throw new UnsupportedOperationException();
  }
}
