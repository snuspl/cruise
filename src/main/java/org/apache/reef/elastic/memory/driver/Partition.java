package org.apache.reef.elastic.memory.driver;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;

/**
 * State of portion of task memory (belonged to specific evaluator)
 * In current version, we only consider data state.
 */
@DriverSide
@Private
final class Partition {
  private final String evalId;

  /* metadata of target task memory */
  private final String dataType;
  int start_offset;
  int length;

  Partition(String evalId, String dataType) {
    this.evalId = evalId;
    this.dataType = dataType;
  }

  public String getEvalId() {
    return evalId;
  }

  public String getDataType() {
    return dataType;
  }
}
