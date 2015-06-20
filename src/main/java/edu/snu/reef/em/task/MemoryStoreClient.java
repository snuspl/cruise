package edu.snu.reef.em.task;

import org.apache.commons.lang.math.IntRange;
import org.apache.reef.annotations.audience.TaskSide;

import java.util.List;
import java.util.Set;

/**
 * Task-side interface of MemoryStoreClient
 */
@TaskSide
public interface MemoryStoreClient {

  /**
   * Register a data item that must not be moved to other evaluators
   *
   * @param key key string that represents a certain data type
   * @param value data item to register
   * @param <T> the actual data type
   */
  <T> void putLocal(String key, T value);

  /**
   * Register data items that must not be moved to other evaluators
   *
   * @param key key string that represents a certain data type
   * @param values list of data items to register
   * @param <T> the actual data type
   */
  <T> void putLocal(String key, List<T> values);

  /**
   * Register a data item that can be migrated around evaluators for job optimization
   *
   * @param key key string that represents a certain data type
   * @param value data item to register
   * @param <T> the actual data type
   */
 <T> void putMovable(String key, T value);

  /**
   * Register data items that can be migrated around evaluators for job optimization
   *
   * @param key key string that represents a certain data type
   * @param values list of data items to register
   * @param <T> the actual data type
   */
  <T> void putMovable(String key, List<T> values);

  /**
   * Fetch data of a certain key from this store
   *
   * @param key key string that represents a certain data type
   * @param <T> the actual data type
   * @return data corresponding to the input key
   */
  <T> List<T> get(String key);

  /**
   * Fetch the global integer ids of data associated with a certain key
   *
   * @param key key string that represents a certain data type
   * @return integer ids of data corresponding to the input key
   */
  Set<IntRange> getIds(String key);

  /**
   * Completely remove data associated with a certain key from this store
   *
   * @param key key string that represents a certain data type
   */
  void remove(String key);

  /**
   * Query about the update status of this store
   */
  boolean hasChanged();
}

