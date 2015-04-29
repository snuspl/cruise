package org.apache.reef.elastic.memory.task;

import org.apache.reef.annotations.audience.TaskSide;
import org.apache.reef.io.data.loading.api.DataSet;

import java.util.HashMap;
import java.util.Map;

/**
 * In-memory data assigned to each evaluator
 *
 * It is not exposed to user
 */
@TaskSide
final class ElasticMemoryData {

  /**
   * Map of dataType and ElasticDataSet
   */
  private final Map<String, ParcelableDataSet> dataMap = new HashMap<String, ParcelableDataSet>();

  /**
   * Ask ElasticMem to return partial of data
   * @param ratio
   * @return
   */
  public Parcel getPartialData(String dataType, float ratio) {
    ParcelableDataSet data = dataMap.get(dataType);
    return data.getParcel(ratio);
  }

  public ParcelableDataSet getData(String dataType) {
    ParcelableDataSet data = dataMap.get(dataType);
    return data;
  }

  public void addData(String dataType, DataSet newData) {
    ParcelableDataSet data = dataMap.get(dataType);
    data.addData(newData);
  }

  public void addData(String dataType, ParcelableDataSet newData) {
    ParcelableDataSet data = dataMap.get(dataType);
    data.mergeDataSet(newData);
  }

  public void delData(String dataType, ParcelableDataSet targetData) {
    ParcelableDataSet data = dataMap.get(dataType);
    data.removeDataSet(targetData);
  }

  public String showDataStatus(String dataType) {
    ParcelableDataSet data = dataMap.get(dataType);
    return data.toString();
  }

  @Override
  public String toString() {
    return dataMap.toString();
  }
}

// Complete overall workflow.