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
package edu.snu.cay.services.ps.common.resolver;

import edu.snu.cay.services.em.driver.api.EMRoutingTableUpdate;
import edu.snu.cay.services.ps.driver.impl.EMRoutingTable;

import java.util.List;

/**
 * A ServerResolver that is queried for the mapping between hashed keys,
 * partitions and servers.
 *
 * Used at both the worker and server:
 * A worker resolves a hashed key to its server,
 * then the server resolves this hashed key to a partition.
 */
public interface ServerResolver {

  /**
   * @param hash unsigned int hash to resolve server from.
   * @return Network Connection Service identifier of the server.
   */
  String resolveServer(int hash);

  /**
   * @param hash unsigned int hash to resolve partition index from.
   * @return Global partition index that this hash resolves to.
   */
  int resolvePartition(int hash);

  /**
   * @param server Network Connections Service identifier of the server.
   * @return List of global partition indices that are mapped to the server.
   */
  List<Integer> getPartitions(String server);

  /**
   * Initialize its local routing table.
   * Note that this method is used only in the dynamic partitioned ParameterServer.
   */
  void initRoutingTable(EMRoutingTable routingTable);

  /**
   * Update its local routing table.
   * @param routingTableUpdate
   */
  void updateRoutingTable(EMRoutingTableUpdate routingTableUpdate);
}
