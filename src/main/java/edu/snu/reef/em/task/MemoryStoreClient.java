package edu.snu.reef.em.task;

import org.apache.reef.annotations.audience.TaskSide;

import java.util.List;

/**
 * Task-side interface of MemoryStoreClient
 */
@TaskSide
public interface MemoryStoreClient {

  /**
   * register data that must not be moved around
   * @param key key string that represents a certain data type
   * @param value data item to register
   * @param <T> the actual data type
   */
  public <T> void putLocal(String key, T value);

  /**
   * register data that must not be moved around
   * @param key key string that represents a certain data type
   * @param values list of data items to register
   * @param <T> the actual data type
   */
  public <T> void putLocal(String key, List<T> values);

  /**
   * register data that can be migrated around for job optimization
   * @param key key string that represents a certain data type
   * @param value data item to register
   * @param <T> the actual data type
   */
  public <T> void putMovable(String key, T value);

  /**
   * register data that can be migrated around for job optimization
   * @param key key string that represents a certain data type
   * @param values list of data items to register
   * @param <T> the actual data type
   */
  public <T> void putMovable(String key, List<T> values);

  /**
   * fetch data of a certain key from this store
   * @param key key string that represents a certain data type
   * @param <T> the actual data type
   * @return data corresponding to the input key
   */
  public <T> List<T> get(String key);

  public void remove(String key);

  /**
   * query driver-side about the update status of this store
   */
  public boolean hasChanged();

}
