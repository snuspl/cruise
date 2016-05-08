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
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.EventHandler;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Driver-side API of ElasticMemoryService.
 */
@DriverSide
@DefaultImplementation(ElasticMemoryImpl.class)
public interface ElasticMemory {

  /**
   * Add new evaluators as specified.
   * @param number number of evaluators to add
   * @param megaBytes memory size of each new evaluator in MB
   * @param cores number of cores of each new evaluator
   * @param evaluatorAllocatedHandler callback which handles {@link AllocatedEvaluator} event
   * @param contextActiveHandlerList callbacks which handle {@link ActiveContext} events, executed in sequence
   */
  void add(int number, int megaBytes, int cores,
           EventHandler<AllocatedEvaluator> evaluatorAllocatedHandler,
           List<EventHandler<ActiveContext>> contextActiveHandlerList);

  /**
   * Release the evaluator specified by a given identifier.
   * @param evalId identifier of the evaluator to release
   * @param callback an application-level callback to be called, or null if no callback is needed
   */
  void delete(String evalId, @Nullable EventHandler<AvroElasticMemoryMessage> callback);

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
   * @param dataTransferredCallback handler to call when data transfer is completed and the migration is waiting
   *                                for a call to {@link #applyUpdates}, or null if no callback is needed
   * @param finishedCallback handler to call when move operation is completed, or null if no callback is needed
   */
  void move(String dataType, Set<LongRange> rangeSet, String srcEvalId, String destEvalId,
            @Nullable EventHandler<AvroElasticMemoryMessage> dataTransferredCallback,
            @Nullable EventHandler<AvroElasticMemoryMessage> finishedCallback);

  /**
   * Move a certain number of units of an evaluator's state to another evaluator.
   *
   * @param dataType data type to perform this operation
   * @param numUnits the number of units to move
   * @param srcEvalId identifier of the source evaluator
   * @param destEvalId identifier of the destination evaluator
   * @param dataTransferredCallback handler to call when data transfer is completed and the migration is waiting
   *                                for a call to {@link #applyUpdates}, or null if no callback is needed
   * @param finishedCallback handler to call when move operation is completed, or null if no callback is needed
   */
  void move(String dataType, int numUnits, String srcEvalId, String destEvalId,
            @Nullable EventHandler<AvroElasticMemoryMessage> dataTransferredCallback,
            @Nullable EventHandler<AvroElasticMemoryMessage> finishedCallback);

  /**
   * Move a certain number of blocks to another Evaluator.
   *
   * @param dataType data type to perform this operation
   * @param numBlocks the number of blocks to move
   * @param srcEvalId identifier of the source evaluator
   * @param destEvalId identifier of the destination evaluator
   * @param finishedCallback handler to call when move operation is completed, or null if no callback is needed
   */
  void move(String dataType, int numBlocks, String srcEvalId, String destEvalId,
            @Nullable EventHandler<AvroElasticMemoryMessage> finishedCallback);

  /**
   * Apply the intermediate changes in EM's states by the migration.
   * To avoid race condition, EM requests for Users to call this method explicitly.
   * But once EM allows remote access to the data, we can remove this barrier
   * letting EM update its state automatically.
   */
  void applyUpdates();

  /**
   * Persist the state of an evaluator into stable storage.
   *
   * @param evalId identifier of the evaluator whose state should be persisted
   */
  void checkpoint(String evalId);

  /**
   * Register a callback for listening updates in EM routing table.
   * @param clientId a client id
   * @param updateCallback a callback
   */
  void registerRoutingTableUpdateCallback(String clientId, EventHandler<EMRoutingTableUpdate> updateCallback);

  /**
   * Deregister a callback for listening updates in EM routing table.
   * @param clientId a client id
   */
  void deregisterRoutingTableUpdateCallback(String clientId);

  /**
   * @return the Driver's view of up-to-date mapping between MemoryStores and blocks.
   */
  Map<Integer, Set<Integer>> getStoreIdToBlockIds();

  /**
   * @return the number of total blocks that exist in this Elastic Memory instance.
   */
  int getNumTotalBlocks();
}
