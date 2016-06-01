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
import edu.snu.cay.services.ps.common.parameters.NumPartitions;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

/**
 * Resolves to the single server node defined by {@link ServerId}.
 */
public final class SingleNodeServerResolver implements ServerResolver {

  /**
   * Network Connection Service identifier of the server.
   */
  private final String serverId;

  /**
   * Number of partitions.
   */
  private final int numPartitions;

  /**
   * List of all partitions mapped to the single node.
   */
  private final List<Integer> partitions;

  @Inject
  private SingleNodeServerResolver(@Parameter(ServerId.class) final String serverId,
                                   @Parameter(NumPartitions.class) final int numPartitions) {
    this.serverId = serverId;
    this.numPartitions = numPartitions;
    this.partitions = new ArrayList<>(numPartitions);
    for (int i = 0; i < numPartitions; i++) {
      partitions.add(i);
    }
  }

  @Override
  public String resolveServer(final int hash) {
    return serverId;
  }

  @Override
  public int resolvePartition(final int hash) {
    return hash % numPartitions;
  }

  @Override
  public List<Integer> getPartitions(final String server) {
    return partitions;
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
