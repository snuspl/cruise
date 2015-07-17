package edu.snu.cay.services.em.driver.api;

import edu.snu.cay.services.em.driver.impl.ElasticMemoryImpl;
import org.apache.commons.lang.math.IntRange;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.Set;

/**
 * Driver-side API of ElasticMemoryService
 */
@DriverSide
@DefaultImplementation(ElasticMemoryImpl.class)
public interface ElasticMemory {

  /**
   * Add new evaluators as specified
   *
   * @param number number of evaluators to add
   * @param megaBytes memory size of each new evaluator in MB
   * @param cores number of cores of each new evaluator
   */
  void add(int number, int megaBytes, int cores);

  /**
   * Release the evaluator specified by a given identifier
   *
   * @param evalId identifier of the evaluator to release
   */
  void delete(String evalId);

  /**
   * Resize the evaluator specified by a given identifier
   *
   * @param evalId identifier of the evaluator to delete
   * @param megaBytes new memory size in MB
   * @param cores new number of cores
   */
  void resize(String evalId, int megaBytes, int cores);

  /**
   * Move a part of an evaluator's state to another evaluator
   *
   * @param dataClassName data type to perform this operation
   * @param rangeSet the range of integer identifiers that specify the state to move
   * @param srcEvalId identifier of the source evaluator
   * @param destEvalId identifier of the destination evaluator
   */
  void move(String dataClassName, Set<IntRange> rangeSet, String srcEvalId, String destEvalId);

  /**
   * Persist the state of an evaluator into stable storage
   *
   * @param evalId identifier of the evaluator whose state should be persisted
   */
  void checkpoint(String evalId);
}
