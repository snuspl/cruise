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
package edu.snu.cay.services.ps.driver.impl;

import edu.snu.cay.services.em.driver.api.EMRoutingTableUpdate;
import edu.snu.cay.services.em.driver.api.ElasticMemory;
import edu.snu.cay.services.ps.avro.AvroParameterServerMsg;
import edu.snu.cay.services.ps.avro.RoutingTableUpdateMsg;
import edu.snu.cay.services.ps.avro.Type;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.*;

/**
 * Provides the routing table information used in Dynamic Partitioned ParameterServer.
 * It receives the latest routing table from EM Driver. In addition, it keeps the mapping between
 * EM's MemoryStore ID and the PS's NCS endpoint id for PSWorkers can send requests to the appropriate servers.
 */
@Private
@DriverSide
public final class EMRoutingTableManager {
  private static final String CLIENT_ID = "PS_CLIENT";

  private final Map<Integer, String> storeIdToEndpointId = new HashMap<>();
  private final ElasticMemory elasticMemory;
  private final InjectionFuture<PSMessageSender> sender;

  @Inject
  EMRoutingTableManager(final ElasticMemory elasticMemory,
                        final InjectionFuture<PSMessageSender> sender) {
    this.elasticMemory = elasticMemory;
    this.sender = sender;
  }

  /**
   * Called when a Server instance based on EM is created. This method adds a relationship between
   * EM's MemoryStore ID and PS's NCS endpoint ID.
   * @param storeId The MemoryStore id in EM.
   * @param endpointId The Endpoint id in PS.
   */
  public void register(final int storeId, final String endpointId) {
    storeIdToEndpointId.put(storeId, endpointId);
  }

  /**
   * @return The EM's routing table to pass to PSWorkers.
   */
  EMRoutingTable getEMRoutingTable() {
    elasticMemory.registerRoutingTableUpdateCallback(CLIENT_ID, new EMRoutingTableUpdateHandler());
    return new EMRoutingTable(
        elasticMemory.getStoreIdToBlockIds(),
        storeIdToEndpointId,
        elasticMemory.getNumTotalBlocks());
  }

  /**
   * A handler of EMRoutingTableUpdate.
   * It broadcasts the update info to all active PS workers
   */
  private final class EMRoutingTableUpdateHandler implements EventHandler<EMRoutingTableUpdate> {
    @Override
    public void onNext(final EMRoutingTableUpdate emRoutingTableUpdate) {
      final int oldOwnerId = emRoutingTableUpdate.getOldOwnerId();
      final int newOwnerId = emRoutingTableUpdate.getNewOwnerId();
      final List<Integer> blockIds = emRoutingTableUpdate.getBlockIds();

      final RoutingTableUpdateMsg routingTableUpdateMsg = RoutingTableUpdateMsg.newBuilder()
          .setOldOwnerId(oldOwnerId)
          .setNewOwnerId(newOwnerId)
          .setBlockIds(blockIds)
          .build();

      final AvroParameterServerMsg updateMsg =
          AvroParameterServerMsg.newBuilder()
              .setType(Type.RoutingTableUpdateMsg)
              .setRoutingTableUpdateMsg(routingTableUpdateMsg)
              .build();

      // broadcast update in routing tables of EM in PS servers to all active PS workers
      for (final String workerId : storeIdToEndpointId.values()) {
        sender.get().send(workerId, updateMsg);
      }
    }
  }
}
