package org.apache.reef.elastic.memory.driver;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;

/**
 * State of portion of task memory (belonged to specific evaluator)
 * In current version, we only consider data state.
 */
@DriverSide
@Private
final class State {
  private final String evalId;
  /* metadata of target task memory */

  State(String evalId) {
    this.evalId = evalId;
  }

  public String getEvalId() {
    return evalId;
  }
}
