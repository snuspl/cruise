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
 * A table initializer class that initializes table at containers in different ways corresponding to their roles:
 * associators, subscribers, containers with no-relationship.
 * 1) {@link #initTableInAssociators} : It initializes a table in associators with information of allocated blocks
 *   and src file, if it exists. Dynamically associated containers do not have any blocks.
 * 2) {@link #initTableInSubscribers}: Table in subscriber containers are initialized by receiving
 *   up-to-date global ownership info that will be cached and updated automatically.
 * 3) {@link #initVoidTable}: Our system provides a way (lookup) for containers to access a table,
 *   even they are not associator nor subscriber. This method initializes a table in those containers
 *   that have no assigned blocks and do not need global ownership table.
 *
 * For all types of containers, initialization info includes basic immutable table configurations
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
   * Initializes a table for associators by providing each allocated blocks.
   * It's a blocking call so that waits until all containers setup corresponding partitions.
   * @param tableConf a configuration of table
   * @param containerIdSet a set of container ids
   * @param containerIdToBlockIdSet allocated block id set for containers
   */
  void initTableInAssociators(final TableConfiguration tableConf,
                              final Set<String> containerIdSet,
                              final Map<String, Set<Long>> containerIdToBlockIdSet) {
    // final Map<String, InputSplit> containerIdToInputSplit;
    if (tableConf.getFilePath().isPresent()) { // if FilePath has been configured
      // obtain InputSplits using hdfs library and assign it to containers
      // and then bind InputFormat impl and bind a serialized InputSplit for each container

      // containerIdToInputSplit = ;
    }

    for (final String containerId : containerIdSet) {
      final Set<Long> localBlockSet = containerIdToBlockIdSet.get(containerId);
      if (localBlockSet != null) {
        //final InputSplit localSplit = containerIdToInputSplit.get(containerId);

//        if (localSplit != null) {
//          // send init msg (tableConf + fileInfo + localBlockSet)
//        } else {
//          // send init msg (tableConf + localBlockSet)
//        }
      } else {

        // send init msg (tableConf) and wait until all table partitions are initialized
      }
    }

    // wait until all table partitions are initialized
  }

  /**
   * Initializes a table for subscribers by providing global ownership information.
   * @param tableConf a configuration of table
   * @param containerIdSet a set of container ids
   */
  void initTableInSubscribers(final TableConfiguration tableConf,
                              final Set<String> containerIdSet,
                              final List<String> blockLocations) {
    // TODO #19: implement subscription mechanism
    for (final String containerId : containerIdSet) {
      // send table init (tableConf + blockLocations) msg to containers
    }

    // subscribers should be registered into MigrationManager so as to retrieve up-to-date ownership info
  }

  /**
   * Initializes the table info in a container that is not associator nor subscriber.
   * It is for the case when that kind of containers try to access the table first time.
   * @param tableConf
   */
  void initVoidTable(final TableConfiguration tableConf,
                     final String containerId) {

    // send init msg (tableConf) and wait response
  }
}
