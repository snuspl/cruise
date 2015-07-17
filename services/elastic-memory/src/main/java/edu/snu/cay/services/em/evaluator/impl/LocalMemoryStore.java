package edu.snu.cay.services.em.evaluator.impl;

import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.math.IntRange;
import org.apache.reef.annotations.audience.EvaluatorSide;

import javax.inject.Inject;
import java.util.*;

@EvaluatorSide
public final class LocalMemoryStore implements MemoryStore {

  private final Map<String, List> localDataMap;

  @Inject
  public LocalMemoryStore() {
    localDataMap = new HashMap<>();
  }

  @Override
  public <T> void putLocal(final String key, final T value) {
    List<Object> singleObjectList = new LinkedList<>();
    singleObjectList.add(value);
    localDataMap.put(key, singleObjectList);
  }

  @Override
  public <T> void putLocal(final String key, final List<T> values) {
    localDataMap.put(key, values);
  }

  @Override
  public <T> void putMovable(final String key, final T value) {
    putLocal(key, value);
  }

  @Override
  public <T> void putMovable(final String key, final List<T> values) {
    putLocal(key, values);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> List<T> get(final String key) {
    return (List<T>)localDataMap.get(key);
  }

  @Override
  public Set<IntRange> getIds(final String key) {
    throw new NotImplementedException();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> List<T> remove(final String key) {
    return localDataMap.remove(key);
  }

  @Override
  public boolean hasChanged() {
    throw new NotImplementedException();
  }
}
