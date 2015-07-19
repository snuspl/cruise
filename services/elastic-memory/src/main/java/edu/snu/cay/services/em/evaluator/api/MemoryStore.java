package edu.snu.cay.services.em.evaluator.api;

import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.io.network.util.Pair;

import java.util.List;

/**
 * Evaluator-side interface of MemoryStore
 */
@EvaluatorSide
public interface MemoryStore {

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
   * @param id global unique identifier of item
   * @param value data item to register
   * @param <T> the actual data type
   */
 <T> void putMovable(String key, long id, T value);

  /**
   * Register data items that can be migrated around evaluators for job optimization
   *
   * @param key key string that represents a certain data type
   * @param ids list of global unique identifiers for each item
   * @param values list of data items to register
   * @param <T> the actual data type
   */
  <T> void putMovable(String key, List<Long> ids, List<T> values);

  /**
   * Fetch all data of a certain key from this store
   *
   * @param key key string that represents a certain data type
   * @param <T> the actual data type
   * @return all data corresponding to the input key
   */
  <T> List<T> get(String key);

  /**
   * Fetch a certain data item from this store.
   *
   * @param key key string that represents a certain data type
   * @param id global unique identifier of item
   * @param <T> the actual data type
   * @return data item and its id
   */
  <T> Pair<Long, T> get(String key, long id);

  /**
   * Fetch and remove a certain data item from this store.
   *
   * @param key key string that represents a certain data type
   * @param id global unique identifier of item
   * @param <T> the actual data type
   * @return data items and their ids
   */
  <T> Pair<Long, T> remove(String key, long id);

  /**
   * Fetch certain data items from this store.
   *
   * @param key key string that represents a certain data type
   * @param startId minimum value of the ids of items to fetch
   * @param endId maximum value of the ids of items to fetch
   * @param <T> the actual data type
   * @return data items and their id
   */
  <T> List<Pair<Long, T>> get(String key, long startId, long endId);

  /**
   * Fetch and remove certain data items from this store.
   *
   * @param key key string that represents a certain data type
   * @param startId minimum value of the ids of items to fetch
   * @param endId maximum value of the ids of items to fetch
   * @param <T> the actual data type
   * @return data items and their id
   */
  <T> List<Pair<Long, T>> remove(String key, long startId, long endId);


  /**
   * Query about the update status of this store
   */
  boolean hasChanged();
}
