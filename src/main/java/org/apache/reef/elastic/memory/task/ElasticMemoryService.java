package org.apache.reef.elastic.memory.task;

import org.apache.reef.annotations.audience.TaskSide;
import org.apache.reef.io.data.loading.api.DataSet;

/**
 * Task-side interface of ElasticMemoryService
 */
@TaskSide
interface ElasticMemoryService {

  /**
   * Stage dataSet into Elastic Memory Service, specifying dataType
   * @param dataType
   * @param dataSet
   */
  public void stageDataSet(String dataType, DataSet dataSet);

  /**
   * return iterator for staged dataSet
   */
  public DataSet getDataSet(String dataType);

  /**
   * Show memory status in string form
   * @return
   */
  public String showMemoryStatus();

  /**
   * Show memory status of specific dataType in string form
   * @param dataType
   * @return
   */
  public String showMemoryStatus(String dataType);

  /**
   * Query driver-side about update status of elastic memory map
   * @return
   */
  public Boolean isChanged();

}

// naive model : BSP, local-reference

// RDD like in-memory abstraction.
// support various applications in future.