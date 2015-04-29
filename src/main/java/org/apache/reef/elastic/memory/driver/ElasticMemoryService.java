package org.apache.reef.elastic.memory.driver;

import org.apache.reef.annotations.audience.DriverSide;

import java.util.List;

/**
 * Driver-side interface of ElasticMemoryService
 */
@DriverSide
interface ElasticMemoryService {

  /**
   * Add a new evaluator that has size
   * @param size
   */
  void add(int size);

  /**
   *
   * @param evalId
   */
  void del(String evalId);

  /**
   *
   * @param evalId
   * @param size
   */
  void resize(String evalId, int size);

  /**
   * Move state to destination evaluator
   *
   * @param partition
   * @param destEvalId
   */
  void move(Partition partition, String destEvalId);

  /**
   * Merge states into single destination evaluator
   * @param partitions
   * @param destEvalId
   */
  void merge(List<Partition> partitions, String destEvalId);

  /**
   * Split state into destEvalIds
   *
   * @param partition
   * @param destEvalIds
   */
  void split(Partition partition, List<String> destEvalIds, HashFunc hashFunc);
}

// add, del, move -> split, merge

// naming
