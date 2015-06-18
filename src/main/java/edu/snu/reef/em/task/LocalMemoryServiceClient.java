package edu.snu.reef.em.task;

import org.apache.reef.annotations.audience.TaskSide;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@TaskSide
public final class LocalMemoryServiceClient implements MemoryStoreClient {

  private final Map<String, List> localDataMap;

  @Inject
  public LocalMemoryServiceClient() {
    localDataMap = new HashMap<>();
  }

  @Override
  public <T> void putLocal(String key, T value) {
    List<Object> singleObjectList = new LinkedList<>();
    singleObjectList.add(value);
    localDataMap.put(key, singleObjectList);
  }

  @Override
  public <T> void putLocal(String key, List<T> values) {
    localDataMap.put(key, values);
  }

  @Override
  @Deprecated
  public <T> void putMovable(String key, T value) {
    putLocal(key, value);
  }

  @Override
  @Deprecated
  public <T> void putMovable(String key, List<T> values) {
    putLocal(key, values);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> List<T> get(String key) {
    return (List<T>)localDataMap.get(key);
  }

  @Override
  public void remove(String key) {
    localDataMap.remove(key);
  }

  @Override
  @Deprecated
  public boolean hasChanged() {
    throw new UnsupportedOperationException();
  }
}
