package org.apache.reef.elastic.memory.task;

import org.apache.reef.annotations.audience.TaskSide;
import org.apache.reef.elastic.memory.Key;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * In-memory data assigned to each evaluator
 *
 * It is not exposed to user
 */
@TaskSide
final class MemoryStorage {

  /**
   * Map of dataType and ElasticDataSet
   */
  private final Map<Class<? extends Key>, List<Object>> dataMap = new HashMap<>();

  public <T> void put(Class<? extends Key<T>> key, T value) {
    if (!dataMap.containsKey(key)) {
      dataMap.put(key, new LinkedList<>());
    }
    dataMap.get(key).add(value);
  }

  public <T> void put(Class<? extends Key<T>> key, List<T> value) {
    if (!dataMap.containsKey(key)) {
      dataMap.put(key, new LinkedList<>());
    }
    dataMap.get(key).addAll(value);
  }

  @SuppressWarnings("unchecked")
  public <T> List<T> get(Class<? extends Key<T>> key) {
    if (dataMap.containsKey(key)) {
      return (List<T>) dataMap.get(key);
    } else {
      return (List<T>) new LinkedList<>();
    }
  }

//  /**
//   * Ask ElasticMem to return partial of data
//   * @param ratio
//   * @return
//   */
//  public Parcel getPartialData(String dataType, float ratio) {
//    ParcelableDataSet data = dataMap.get(dataType);
//    return data.getParcel(ratio);
//  }
//
//  public ParcelableDataSet getData(String dataType) {
//    ParcelableDataSet data = dataMap.get(dataType);
//    return data;
//  }
//
//  public void addData(String dataType, DataSet newData) {
//    ParcelableDataSet data = dataMap.get(dataType);
//    data.addData(newData);
//  }
//
//  public void addData(String dataType, ParcelableDataSet newData) {
//    ParcelableDataSet data = dataMap.get(dataType);
//    data.mergeDataSet(newData);
//  }
//
//  public void delData(String dataType, ParcelableDataSet targetData) {
//    ParcelableDataSet data = dataMap.get(dataType);
//    data.removeDataSet(targetData);
//  }
//
//  public String showDataStatus(String dataType) {
//    ParcelableDataSet data = dataMap.get(dataType);
//    return data.toString();
//  }
//
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(MemoryStorage.class.getName())
        .append("[")
        .append(dataMap.toString())
        .append("]");
    return sb.toString();
  }
}
