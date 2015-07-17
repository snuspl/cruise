package edu.snu.reef.em.evaluator.impl;

import edu.snu.reef.em.evaluator.api.MemoryStore;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.math.IntRange;
import org.apache.reef.annotations.audience.EvaluatorSide;

import javax.inject.Inject;
import java.util.*;

@EvaluatorSide
public final class ElasticMemoryStore implements MemoryStore {

  private final Map<String, List> localDataMap;
  private final Map<String, List> elasticDataMap;

  @Inject
  public ElasticMemoryStore() {
    localDataMap = new HashMap<>();
    elasticDataMap = new HashMap<>();
  }

  @Override
  public <T> void putLocal(final String key, final T value) {
    final List<Object> singleObjectList = new LinkedList<>();
    singleObjectList.add(value);
    localDataMap.put(key, singleObjectList);
  }

  @Override
  public <T> void putLocal(final String key, final List<T> values) {
    localDataMap.put(key, values);
  }

  @Override
  public <T> void putMovable(final String key, final T value) {
    final List<Object> singleObjectList = new LinkedList<>();
    singleObjectList.add(value);
    elasticDataMap.put(key, singleObjectList);
  }

  @Override
  public <T> void putMovable(final String key, final List<T> values) {
    elasticDataMap.put(key, values);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> List<T> get(final String key) {
    final List<T> retList = new LinkedList<>();
    if (localDataMap.containsKey(key)) {
      retList.addAll((List<T>)localDataMap.get(key));
    }
    if (elasticDataMap.containsKey(key)) {
      retList.addAll((List<T>)elasticDataMap.get(key));
    }
    return retList;
  }

  @Override
  public Set<IntRange> getIds(final String key) {
    throw new NotImplementedException();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> List<T> remove(final String key) {
    final List<T> retList = new LinkedList<>();
    if (localDataMap.containsKey(key)) {
      retList.addAll((List<T>)localDataMap.remove(key));
    }
    if (elasticDataMap.containsKey(key)) {
      retList.addAll((List<T>)elasticDataMap.remove(key));
    }
    return retList;
  }

  @Override
  public boolean hasChanged() {
    throw new NotImplementedException();
  }
}
