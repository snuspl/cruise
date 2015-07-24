package edu.snu.cay.services.em.evaluator.api;

import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.io.network.util.Pair;

import java.util.List;
import java.util.Map;

/**
 * Evaluator-side interface of SubMemoryStore.
 */
@EvaluatorSide
public interface SubMemoryStore {

  /**
   * Register a data item of a certain data type to this store.
   *
   * @param dataType string that represents a certain data type
   * @param id global unique identifier of item
   * @param value data item to register
   * @param <T> actual data type
   */
  <T> void put(String dataType, long id, T value);

  /**
   * Register data items of a certain data type to this store.
   *
   * @param dataType string that represents a certain data type
   * @param ids list of global unique identifiers for each item
   * @param values list of data items to register
   * @param <T> actual data type
   */
  <T> void putList(String dataType, List<Long> ids, List<T> values);

  /**
   * Fetch a certain data item from this store.
   *
   * @param dataType string that represents a certain data type
   * @param id global unique identifier of item
   * @param <T> actual data type
   * @return data id and the actual data item
   */
  <T> Pair<Long, T> get(String dataType, long id);

  /**
   * Fetch all data of a certain data type from this store.
   * The returned list is a shallow copy of the internal data structure.
   *
   * @param dataType string that represents a certain data type
   * @param <T> actual data type
   * @return map of data ids and the corresponding data items
   */
  <T> Map<Long, T> getAll(String dataType);

  /**
   * Fetch data items from this store whose ids are between {@code startId} and {@code endId}, both inclusive.
   * The returned list is a shallow copy of the internal data structure.
   *
   * @param dataType string that represents a certain data type
   * @param startId minimum value of the ids of items to fetch
   * @param endId maximum value of the ids of items to fetch
   * @param <T> actual data type
   * @return map of data ids and the corresponding data items
   */
  <T> Map<Long, T> getRange(String dataType, long startId, long endId);

  /**
   * Fetch and remove a certain data item from this store.
   *
   * @param dataType string that represents a certain data type
   * @param id global unique identifier of item
   * @param <T> actual data type
   * @return data id and the actual data item
   */
  <T> Pair<Long, T> remove(String dataType, long id);

  /**
   * Fetch and remove all data of a certain data type from this store.
   * A {@code getAll(dataType)} after this method will return null.
   *
   * @param dataType string that represents a certain data type
   * @param <T> actual data type
   * @return map of data ids and the corresponding data items
   */
  <T> Map<Long, T> removeAll(String dataType);

  /**
   * Fetch and remove data items from this store
   * whose ids are between {@code startId} and {@code endId}, both inclusive.
   *
   * @param dataType string that represents a certain data type
   * @param startId minimum value of the ids of items to fetch
   * @param endId maximum value of the ids of items to fetch
   * @param <T> actual data type
   * @return map of data ids and the corresponding data items
   */
  <T> Map<Long, T> removeRange(String dataType, long startId, long endId);


  /**
   * Query about the update status of this store
   */
  boolean hasChanged();
}
