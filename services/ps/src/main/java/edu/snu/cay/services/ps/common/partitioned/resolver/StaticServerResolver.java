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
  private final int numPartitions;
  private final String[] servers;
  private final Map<String, List<Integer>> partitions;

  @Inject
  private StaticServerResolver(@Parameter(NumServers.class) final int numServers,
                               @Parameter(NumPartitions.class) final int numPartitions) {
    this.numPartitions = numPartitions;

    this.partitions = new HashMap<>(numServers);
    for (int i = 0; i < numServers; i++) {
      partitions.put(SERVER_ID_PREFIX + i, new ArrayList<Integer>());
    }

    this.servers = new String[numPartitions];
    for (int virtualNodeIndex = 0; virtualNodeIndex < servers.length; virtualNodeIndex++) {
      final int serverIndex = virtualNodeIndex % numServers;
      servers[virtualNodeIndex] = (SERVER_ID_PREFIX + serverIndex);
      partitions.get(SERVER_ID_PREFIX + serverIndex).add(virtualNodeIndex);
    }
  }

  @Override
  public String resolveServer(final int hash) {
    return servers[resolvePartition(hash)];
  }

  @Override
  public int resolvePartition(final int hash) {
    return hash % numPartitions;
  }

  @Override
  public List<Integer> getPartitions(final String server) {
    return partitions.get(server);
  }
}
