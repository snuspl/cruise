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

  private final Map<Integer, String> storeIdToEndpointId = new HashMap<>(); // server-side id mapping
  private final ElasticMemory elasticMemory;
  private final InjectionFuture<PSMessageSender> sender;

  private final Set<String> activeWorkerIds = new HashSet<>();

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
  public synchronized void registerServer(final int storeId, final String endpointId) {
    storeIdToEndpointId.put(storeId, endpointId);
  }

  /**
   * TODO #473: invoke this method when deleting memory store
   * Called when a Server instance based on EM is deleted.
   * This method cleans up existing metadata for the deleted server.
   * @param storeId The MemoryStore id in EM.
   */
  public void deregisterServer(final int storeId) {
    storeIdToEndpointId.remove(storeId);
  }

  /**
   * Registers an worker, {@code workerId} to be notified about updates in the routing table.
   * It also returns the PS server-side EM's routing table to pass it to an initiating PS worker.
   * @param workerId an worker id
   * @return The server-side EM's routing table
   */
  synchronized EMRoutingTable registerWorker(final String workerId) {
    if (activeWorkerIds.isEmpty()) {
      elasticMemory.registerRoutingTableUpdateCallback(CLIENT_ID, new EMRoutingTableUpdateHandler());
    }
    activeWorkerIds.add(workerId);
    return new EMRoutingTable(
        elasticMemory.getStoreIdToBlockIds(),
        storeIdToEndpointId,
        elasticMemory.getNumTotalBlocks());
  }

  /**
   * Deregisters an worker, {@code workerId} when the worker stops working.
   * After invoking this method, the worker will not be notified with the further update of the routing table.
   * @param workerId an worker id
   */
  synchronized void deregisterWorker(final String workerId) {
    activeWorkerIds.remove(workerId);
    if (activeWorkerIds.isEmpty()) {
      elasticMemory.deregisterRoutingTableUpdateCallback(CLIENT_ID);
    }
  }

  /**
   * Broadcasts update in routing tables of EM in PS servers to all active PS workers.
   */
  private synchronized void broadcastMsg(final AvroParameterServerMsg updateMsg) {
    for (final String workerId : activeWorkerIds) {
      sender.get().send(workerId, updateMsg);
    }
  }

  /**
   * A handler of EMRoutingTableUpdate.
   * It broadcasts the update info to all active PS workers.
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

      broadcastMsg(updateMsg);
    }
  }
}
