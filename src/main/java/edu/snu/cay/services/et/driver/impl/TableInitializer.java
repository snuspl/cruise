/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.cay.services.et.driver.impl;

import edu.snu.cay.services.et.configuration.TableConfiguration;
import org.apache.reef.annotations.audience.DriverSide;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A table initializer class that initializes table at executors in different ways corresponding to their roles:
 * associators, subscribers, executors with no-relationship.
 * 1) {@link #initTableInAssociators} : It initializes a table in associators with information of allocated blocks
 *   and src file, if it exists. Dynamically associated executors do not have any blocks.
 * 2) {@link #initTableInSubscribers}: Table in subscriber executors are initialized by receiving
 *   up-to-date global ownership info that will be cached and updated automatically.
 * 3) {@link #initVoidTable}: Our system provides a way (lookup) for executors to access a table,
 *   even they are not associator nor subscriber. This method initializes a table in those executors
 *   that have no assigned blocks and do not need global ownership table.
 *
 * For all types of executors, initialization info includes basic immutable table configurations
 * (e.g., key/value codec, partition function, num total blocks).
 *
 * TODO #10: implement TableInitializer
 */
@DriverSide
final class TableInitializer {

  @Inject
  private TableInitializer() {
  }

  /**
   * Initializes a table for associated executors by providing each allocated blocks.
   * It's a blocking call so that waits until all executors setup corresponding tablets.
   * @param tableConf a configuration of table
   * @param executorIdSet a set of executor ids
   * @param executorIdToBlockIdSet allocated block id set for executors
   */
  void initTableInAssociators(final TableConfiguration tableConf,
                              final Set<String> executorIdSet,
                              final Map<String, Set<Long>> executorIdToBlockIdSet) {
    // final Map<String, InputSplit> executorIdToInputSplit;
    if (tableConf.getFilePath().isPresent()) { // if FilePath has been configured
      // obtain InputSplits using hdfs library and assign it to executors
      // and then bind InputFormat impl and bind a serialized InputSplit for each executor

      // executorIdToInputSplit = ;
    }

    for (final String executorId : executorIdSet) {
      final Set<Long> localBlockSet = executorIdToBlockIdSet.get(executorId);
      if (localBlockSet != null) {
        //final InputSplit localSplit = executorIdToInputSplit.get(executorId);

//        if (localSplit != null) {
//          // send init msg (tableConf + fileInfo + localBlockSet)
//        } else {
//          // send init msg (tableConf + localBlockSet)
//        }
      } else {

        // send init msg (tableConf) and wait until all tablets are initialized
      }
    }

    // wait until all table partitions are initialized
  }

  /**
   * Initializes a table for subscribers by providing global ownership information.
   * @param tableConf a configuration of table
   * @param executorIdSet a set of executor ids
   */
  void initTableInSubscribers(final TableConfiguration tableConf,
                              final Set<String> executorIdSet,
                              final List<String> blockLocations) {
    // TODO #19: implement subscription mechanism
    for (final String executorId : executorIdSet) {
      // send table init (tableConf + blockLocations) msg to executors
    }

    // subscribers should be registered into MigrationManager so as to retrieve up-to-date ownership info
  }

  /**
   * Initializes the table info in a executor that is not associator nor subscriber.
   * It is for the case when that kind of executors try to access the table first time.
   * @param tableConf
   */
  void initVoidTable(final TableConfiguration tableConf,
                     final String executorId) {

    // send init msg (tableConf) and wait response
  }
}
