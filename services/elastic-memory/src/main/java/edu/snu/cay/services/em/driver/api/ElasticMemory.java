/*
 * Copyright (C) 2015 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.cay.services.em.driver.api;

import edu.snu.cay.services.em.avro.AvroElasticMemoryMessage;
import edu.snu.cay.services.em.driver.impl.ElasticMemoryImpl;
import org.apache.commons.lang.math.LongRange;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.EventHandler;

import javax.annotation.Nullable;
import java.util.Set;

/**
 * Driver-side API of ElasticMemoryService.
 */
@DriverSide
@DefaultImplementation(ElasticMemoryImpl.class)
public interface ElasticMemory {

  /**
   * Add new evaluators as specified.
   *
   * @param number number of evaluators to add
   * @param megaBytes memory size of each new evaluator in MB
   * @param cores number of cores of each new evaluator
   */
  void add(int number, int megaBytes, int cores);

  /**
   * Release the evaluator specified by a given identifier.
   *
   * @param evalId identifier of the evaluator to release
   */
  void delete(String evalId);

  /**
   * Resize the evaluator specified by a given identifier.
   *
   * @param evalId identifier of the evaluator to delete
   * @param megaBytes new memory size in MB
   * @param cores new number of cores
   */
  void resize(String evalId, int megaBytes, int cores);

  /**
   * Move specific partitions of an evaluator's state to another evaluator.
   *
   * @param dataType data type to perform this operation
   * @param rangeSet the range of integer identifiers that specify the state to move
   * @param srcEvalId identifier of the source evaluator
   * @param destEvalId identifier of the destination evaluator
   * @param callback handler to call when move operation is completed, or null if no handler is needed
   */
  void move(String dataType, Set<LongRange> rangeSet, String srcEvalId, String destEvalId,
            @Nullable EventHandler<AvroElasticMemoryMessage> callback);

  /**
   * Move a certain number of units of an evaluator's state to another evaluator.
   *
   * @param dataType data type to perform this operation
   * @param unitNum the number of units to move
   * @param srcEvalId identifier of the source evaluator
   * @param destEvalId identifier of the destination evaluator
   * @param callback handler to call when move operation is completed, or null if no handler is needed
   */
  void move(String dataType, int unitNum, String srcEvalId, String destEvalId,
            @Nullable EventHandler<AvroElasticMemoryMessage> callback);

  /**
   * Persist the state of an evaluator into stable storage.
   *
   * @param evalId identifier of the evaluator whose state should be persisted
   */
  void checkpoint(String evalId);
}
