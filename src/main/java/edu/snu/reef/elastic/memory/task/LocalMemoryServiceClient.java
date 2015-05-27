package edu.snu.reef.elastic.memory.task;

import edu.snu.reef.elastic.memory.Key;
import org.apache.reef.annotations.audience.TaskSide;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@TaskSide
public final class LocalMemoryServiceClient implements MemoryStoreClient {

  private final Map<Class<? extends Key>, List<Object>> localDataMap;

  @Inject
  public LocalMemoryServiceClient() {
    localDataMap = new HashMap<>();
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
  @Deprecated
  public <T> void putMovable(Class<? extends Key<T>> key, T value) {
    putLocal(key, value);
  }

  @Override
  @Deprecated
  public <T> void putMovable(Class<? extends Key<T>> key, List<T> values) {
    putLocal(key, values);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> List<T> get(Class<? extends Key<T>> key) {
    return (List<T>)localDataMap.get(key);
  }

  @Override
  @Deprecated
  public boolean hasChanged() {
    throw new UnsupportedOperationException();
  }
}
