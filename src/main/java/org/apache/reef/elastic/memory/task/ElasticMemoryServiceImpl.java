package org.apache.reef.elastic.memory.task;

import org.apache.reef.io.data.loading.api.DataSet;

public class ElasticMemoryServiceImpl implements ElasticMemoryService {

  private final ElasticMem elasticMem = new ElasticMem();

  /**
   * Stage dataSet into Elastic Memory Service
   *
   * @param dataSet
   */
  @Override
  public void stageMemory(DataSet dataSet) {
    elasticMem.addData(dataSet);
  }

  /**
   * return iterator for staged dataSet
   */
  @Override
  public DataSet getDataSet() {
    return elasticMem.getData();
  }

  /**
   * Show memory status in string form
   *
   * @return
   */
  @Override
  public String showMemoryStatus() {
    return elasticMem.toString();
  }

  /**
   * Query driver-side serivce about update status of elastic memory map
   *
   * @return
   */
  @Override
  public Boolean isChanged() {
    return null;
  }
}
