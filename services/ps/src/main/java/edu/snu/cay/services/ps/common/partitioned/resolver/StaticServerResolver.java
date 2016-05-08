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
package edu.snu.cay.services.ps.common.partitioned.resolver;

import edu.snu.cay.services.em.driver.api.EMRoutingTableUpdate;
import edu.snu.cay.services.ps.driver.impl.EMRoutingTable;
import edu.snu.cay.services.ps.common.partitioned.parameters.NumServers;
import edu.snu.cay.services.ps.common.partitioned.parameters.NumPartitions;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static edu.snu.cay.services.ps.common.Constants.SERVER_ID_PREFIX;

/**
 * A hashed key to partition to server mapping that is configured on Job startup,
 * based on a simple round-robin assignment of partitions to physical servers.
 * For example, with 3 servers and 7 partitions, the server-to-partition mapping will produce:
 *   server0 -> 0, 3, 6
 *   server1 -> 1, 4
 *   server2 -> 2, 5
 *
 * This mapping cannot be changed dynamically (i.e., after Job configuration).
 */
public final class StaticServerResolver implements ServerResolver {

  /**
   * Number of partitions, globally held by all servers.
   */
  private final int numPartitions;

  /**
   * Mapping from partition to server.
   * The array is indexed by partition index, and contains the server's NCS name.
   */
  private final String[] partitionToServer;

  /**
   * Mapping from server to partitions.
   * The key is the server's NCS name, and the value is a list of partition indices held by the server.
   * Contains the same information as partitionToServer, but indexed by server.
   */
  private final Map<String, List<Integer>> serverToPartitions;

  @Inject
  private StaticServerResolver(@Parameter(NumServers.class) final int numServers,
                               @Parameter(NumPartitions.class) final int numPartitions) {
    this.numPartitions = numPartitions;

    this.serverToPartitions = new HashMap<>(numServers);
    for (int i = 0; i < numServers; i++) {
      serverToPartitions.put(SERVER_ID_PREFIX + i, new ArrayList<Integer>());
    }

    this.partitionToServer = new String[numPartitions];
    for (int partitionIndex = 0; partitionIndex < partitionToServer.length; partitionIndex++) {
      final int serverIndex = partitionIndex % numServers;
      partitionToServer[partitionIndex] = (SERVER_ID_PREFIX + serverIndex);
      serverToPartitions.get(SERVER_ID_PREFIX + serverIndex).add(partitionIndex);
    }
  }

  @Override
  public String resolveServer(final int hash) {
    return partitionToServer[resolvePartition(hash)];
  }

  @Override
  public int resolvePartition(final int hash) {
    return hash % numPartitions;
  }

  @Override
  public List<Integer> getPartitions(final String server) {
    return serverToPartitions.get(server);
  }

  @Override
  public void initRoutingTable(final EMRoutingTable routingTable) {
    throw new UnsupportedOperationException("This method is used only in the dynamic partitioned PS");
  }

  @Override
  public void updateRoutingTable(final EMRoutingTableUpdate routingTableUpdate) {
    throw new UnsupportedOperationException("This method is used only in the dynamic partitioned PS");
  }
}
