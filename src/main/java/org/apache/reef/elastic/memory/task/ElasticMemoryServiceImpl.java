package org.apache.reef.elastic.memory.task;

import org.apache.reef.io.data.loading.api.DataSet;

public class ElasticMemoryServiceImpl implements ElasticMemoryService {

  private final ElasticMemoryData elasticMemoryData = new ElasticMemoryData();

  /**
   * Stage dataSet into Elastic Memory Service, specifying dataType
   *
   * @param dataType
   * @param dataSet
   */
  @Override
  public void stageDataSet(String dataType, DataSet dataSet) {
    if(dataType == null) {
      dataType = "default";
    }
    elasticMemoryData.addData(dataType, dataSet);
  }

  /**
   * return staged dataSet
   */
  @Override
  public DataSet getDataSet(String dataType) {
    if(dataType == null) {
      dataType = "default";
    }
    return elasticMemoryData.getData(dataType);
  }

  /**
   * Show memory status in string form
   *
   * @return
   */
  @Override
  public String showMemoryStatus() {
    return elasticMemoryData.toString();
  }

  /**
   * Show memory status of specific dataType in string form
   *
   * @param dataType
   * @return
   */
  @Override
  public String showMemoryStatus(String dataType) {
    return elasticMemoryData.showDataStatus(dataType);
  }

  /**
   * Query driver-side about update status of elastic memory map
   *
   * @return
   */
  @Override
  public Boolean isChanged() {
    return null;
  }
}
