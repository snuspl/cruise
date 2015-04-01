package org.apache.reef.elastic.memory.task;

import org.apache.reef.annotations.audience.TaskSide;
import org.apache.reef.io.data.loading.api.DataSet;

/**
 * Task-side interface of ElasticMemoryService
 */
@TaskSide
interface ElasticMemoryService {

  /**
   * Stage dataSet into Elastic Memory Service
   * @param dataSet
   */
  public void stageMemory(DataSet dataSet);

  /**
   * return iterator for staged dataSet
   */
  public DataSet getDataSet();

  /**
   * Show memory status in string form
   * @return
   */
  public String showMemoryStatus();

  /**
   * Query driver-side about update status of elastic memory map
   * @return
   */
  public Boolean isChanged();

}
