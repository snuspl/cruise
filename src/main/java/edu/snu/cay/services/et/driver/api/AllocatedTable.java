/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.cay.services.et.driver.api;

import edu.snu.cay.services.et.common.util.concurrent.ListenableFuture;
import edu.snu.cay.services.et.configuration.TableConfiguration;
import edu.snu.cay.services.et.driver.impl.AllocatedTableImpl;
import edu.snu.cay.services.et.driver.impl.MigrationResult;
import edu.snu.cay.services.et.exceptions.NotAssociatedException;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.*;

/**
 * Represents a state where the table is completely allocated into executors.
 * Now the executors that associate this table have the actual reference of it, and the executors subscribing it
 * receive the ownership information whenever there is any change.
 *
 * Even the table is distributed to executors already, more executors are allowed to subscribe or associate with it,
 * then the changes will be applied to executors immediately.
 */
@DriverSide
@DefaultImplementation(AllocatedTableImpl.class)
public interface AllocatedTable {

  /**
   * @return an identifier of the table
   */
  String getId();

  /**
   * @return a configuration of the table
   */
  TableConfiguration getTableConfiguration();

  /**
   * @return a set of executors associated with the table
   */
  Set<String> getAssociatedExecutorIds();

  /**
   * Returns a partition information of this table.
   * @return a map between an executor id and a set of block ids
   */
  Map<String, Set<Integer>> getPartitionInfo();

  /**
   * @return a list that contains block ownership status. Index of list is block Id.
   */
  List<String> getOwnershipStatus();

  /**
   * @param blockId id of the block to resolve its owner
   * @return the owner's id of the block
   */
  String getOwnerId(int blockId);

  /**
   * Initializes {@link this} by allocating a table
   * with a given {@code tableConfiguration} to {@code initialAssociators}.
   * This method should be called once before other methods.
   * @param tableConfiguration a table configuration
   * @param initialAssociators a list of initial executors to be associated to a table
   * @return a {@link ListenableFuture} for notifying the completion
   */
  ListenableFuture<?> init(TableConfiguration tableConfiguration,
                           List<AllocatedExecutor> initialAssociators);

  /**
   * Loads an input data into a table with executors. Note that this method must be called after table init.
   * @param executors a list of executors which will load data
   * @param inputPath a data file path
   */
  ListenableFuture<?> load(List<AllocatedExecutor> executors, String inputPath);

  /**
   * Subscribes the table. The executors will receive the updates in ownership information for this table.
   * @param executors a list of executors
   */
  ListenableFuture<?> subscribe(List<AllocatedExecutor> executors);

  /**
   * Unsubscribes the table. The executor will not receive ownership update of table anymore.
   * @param executorId an executor id
   */
  ListenableFuture<?> unsubscribe(String executorId);

  /**
   * Associates with the table. The executors will take some blocks of this table.
   * @param executors a list of executors
   */
  ListenableFuture<?> associate(List<AllocatedExecutor> executors);

  /**
   * Decouples the table from the executor. As a result, the executor will not have any blocks nor receive any updates
   * of ownership information.
   * Also, it should confirm that ownership cache of other executors does not have entry of the unassociated executor.
   * Note that all blocks of the table should be emptied out first, before this method is called.
   * @param executorId id of the executor to un-associate with the table
   */
  ListenableFuture<?> unassociate(String executorId);

  /**
   * Moves the {@code numBlocks} number of blocks from src executor to dst executor.
   * Cannot proceed when there're ongoing checkpoints by {@link #checkpoint()}.
   * This call will wait until ongoing checkpoints are completed.
   * @param srcExecutorId an id of src executor
   * @param dstExecutorId an id of dst executor
   * @param numBlocks the number of blocks to move
   * @return a future with migration result
   */
  ListenableFuture<MigrationResult> moveBlocks(String srcExecutorId,
                                               String dstExecutorId,
                                               int numBlocks) throws NotAssociatedException;

  /**
   * Drops {@link this} table by removing tablets and table metadata from all executors.
   * This method should be called after initialized.
   * After this method, the table is completely removed from the system (e.g., master and executors).
   */
  ListenableFuture<?> drop();

  /**
   * Save a snapshot of current status of table.
   * Cannot proceed when there're ongoing migrations by {@link #moveBlocks}.
   * This call will wait until ongoing migrations are completed.
   * @return a future with a checkpoint Id
   */
  ListenableFuture<String> checkpoint();
}
