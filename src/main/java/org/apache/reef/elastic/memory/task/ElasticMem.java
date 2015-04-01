package org.apache.reef.elastic.memory.task;

import org.apache.reef.annotations.audience.TaskSide;
import org.apache.reef.io.data.loading.api.DataSet;

import java.util.HashMap;
import java.util.Map;

/**
 * Memory assigned to each evaluator
 *
 * Currently, support single data set
 *
 * It is not exposed to user
 */
@TaskSide
final class ElasticMem {

  private final ElasticDataSet data = new ElasticDataSet();

  /**
   * Ask ElasticMem to return partial of data
   * @param ratio
   * @return
   */
  public ElasticDataSet getPartialData(float ratio) {
    return null;
  }

  public ElasticDataSet getData() {
    return data;
  }

  public void addData(DataSet data) {
    this.data.addData(data);
  }

  public void addData(ElasticDataSet data) {
    this.data.mergeDataSet(data);
  }

  public void delData(ElasticDataSet data) {
    this.data.removeDataSet(data);
  }

  @Override
  public String toString() {
    return data.toString();
  }
}
