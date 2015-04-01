package org.apache.reef.elastic.memory.driver;

import org.apache.reef.annotations.audience.DriverSide;

/**
 * Driver-side interface of ElasticMemoryService
 */
@DriverSide
interface ElasticMemoryService {

  /**
   * Move state to destination evaluator
   *
   * @param state
   * @param destEvalId
   */
  void move(State state, String destEvalId);

  /**
   * Merge state0 and state1 into destEvalId
   * @param state0
   * @param state1
   * @param destEvalId
   */
  void merge(State state0, State state1, String destEvalId);

  /**
   * Split state into destEvalId0 and destEvalId1
   *
   * @param state
   * @param destEvalId0
   * @param destEvalId1
   */
  void split(State state, String destEvalId0, String destEvalId1);
}
