package edu.snu.reef.elastic.memory.task;

import org.apache.commons.lang.NotImplementedException;
import org.apache.reef.annotations.audience.TaskSide;
import edu.snu.reef.elastic.memory.Key;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@TaskSide
public final class ElasticMemoryServiceClient implements MemoryStoreClient {

  private final Map<Class<? extends Key>, List<Object>> localDataMap;
  private final Map<Class<? extends Key>, List<Object>> elasticDataMap;

  @Inject
  public ElasticMemoryServiceClient() {
    localDataMap = new HashMap<>();
    elasticDataMap = new HashMap<>();
  }

  @Override
  public <T> void putLocal(Class<? extends Key<T>> key, T value) {
    List<Object> singleObjectList = new LinkedList<>();
    singleObjectList.add(value);
    localDataMap.put(key, singleObjectList);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> void putLocal(Class<? extends Key<T>> key, List<T> values) {
    localDataMap.put(key, (List<Object>)values);
  }

  @Override
  public <T> void putMovable(Class<? extends Key<T>> key, T value) {
    List<Object> singleObjectList = new LinkedList<>();
    singleObjectList.add(value);
    elasticDataMap.put(key, singleObjectList);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> void putMovable(Class<? extends Key<T>> key, List<T> values) {
    elasticDataMap.put(key, (List<Object>)values);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> List<T> get(Class<? extends Key<T>> key) {
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
  public boolean hasChanged() {
    throw new NotImplementedException();
  }
}
