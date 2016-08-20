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
import edu.snu.cay.services.ps.avro.*;
import edu.snu.cay.services.ps.common.parameters.NumServers;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Provides the routing table information used in Dynamic ParameterServer.
 * It receives the latest routing table from EM Driver. In addition, it keeps the mapping between
 * EM's MemoryStore ID and the PS's NCS endpoint id for PSWorkers can send requests to the appropriate servers.
 */
// TODO #553: Should be instantiated only when dynamic PS is used.
@Private
@DriverSide
public final class EMRoutingTableManager {
  private static final Logger LOG = Logger.getLogger(EMRoutingTableManager.class.getName());

  private static final String CLIENT_ID = "PS_CLIENT";

  /**
   * A mapping between store id and endpoint id of server-side evaluators.
   */
  private final Map<Integer, String> storeIdToEndpointId = new HashMap<>();

  /**
   * Server-side ElasticMemory instance.
   */
  private final ElasticMemory serverEM;

  private final InjectionFuture<PSMessageSender> sender;

  /**
   * Workers participating in the system and subscribing the routing table of servers.
   */
  private final Set<String> activeWorkerIds = Collections.synchronizedSet(new HashSet<>());

  /**
   * Workers waiting for the server initialization to receive the initial routing table for resolving servers.
   */
  private final Set<String> waitingWorkers = Collections.synchronizedSet(new HashSet<>());

  /**
   * Ongoing sync operations that assures workers to be ready for the deletion of a certain server.
   * Key is a id of server to be deleted and value is a latch for counting the number of synchronized workers.
   */
  private final ConcurrentMap<String, CountDownLatch> ongoingSyncs = new ConcurrentHashMap<>();

  /**
   * The number of initial servers, which is for deciding when to send the routing table to workers at the beginning.
   */
  private final int numInitServers;

  /**
   * A boolean flag representing whether all the initial servers are registered.
   */
  private volatile boolean initialServersReady = false;

  @Inject
  EMRoutingTableManager(final ElasticMemory serverEM,
                        @Parameter(NumServers.class) final int numServers,
                        final InjectionFuture<PSMessageSender> sender) {
    this.serverEM = serverEM;
    this.numInitServers = numServers;
    this.sender = sender;
  }

  /**
   * Called when a Server instance based on EM is created. This method adds a relationship between
   * EM's MemoryStore ID and PS's NCS endpoint ID.
   * If all the initial servers are registered, it sends
   * the PS server-side EM's routing table to all waiting {@code ParameterWorker}s.
   * @param storeId The MemoryStore id in EM.
   * @param endpointId The Endpoint id in PS.
   */
  public synchronized void registerServer(final int storeId, final String endpointId) {
    storeIdToEndpointId.put(storeId, endpointId);
    if (!initialServersReady && storeIdToEndpointId.size() == numInitServers) {
      initialServersReady = true;

      final EMRoutingTable routingTable = new EMRoutingTable(serverEM.getStoreIdToBlockIds(), storeIdToEndpointId);

      for (final String workerId : waitingWorkers) {
        sendWorkerRegisterReplyMsg(workerId, routingTable);
      }
      waitingWorkers.clear(); // will not use this data structure any more
    }
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
   * Sync workers to be ready for the deletion of a certain by receiving updates for
   * their {@link edu.snu.cay.services.ps.common.resolver.DynamicServerResolver}.
   * @param serverId id of server to be deleted
   */
  public synchronized void syncWorkers(final String serverId) {
    if (ongoingSyncs.containsKey(serverId)) {
      LOG.log(Level.WARNING, "Sync for {0} is already ongoing", serverId);
      return;
    }

    final CountDownLatch syncLatch = new CountDownLatch(activeWorkerIds.size());
    ongoingSyncs.put(serverId, syncLatch);

    LOG.log(Level.INFO, "Send sync msg for {0} to workers: {1}", new Object[]{serverId, activeWorkerIds});
    broadcastSyncMsg(serverId);

    try {
      syncLatch.await();
    } catch (final InterruptedException e) {
      LOG.log(Level.WARNING, "Interrupted while waiting for synchronization", e);
    }
  }

  private void broadcastSyncMsg(final String serverId) {
    final RoutingTableSyncMsg routingTableSyncMsg = RoutingTableSyncMsg.newBuilder()
        .setServerId(serverId)
        .build();

    final AvroPSMsg syncMsg =
        AvroPSMsg.newBuilder()
            .setType(Type.RoutingTableSyncMsg)
            .setRoutingTableSyncMsg(routingTableSyncMsg)
            .build();

    for (final String workerId : activeWorkerIds) {
      sender.get().send(workerId, syncMsg);
    }
  }

  synchronized void onSyncReply(final String serverId, final String workerId) {
    if (!ongoingSyncs.containsKey(serverId)) {
      LOG.log(Level.WARNING, "Sync for {0} is already completed", serverId);
      return;
    }

    LOG.log(Level.INFO, "Sync reply for {0} has been arrived from worker {1}", new Object[]{serverId, workerId});
    final CountDownLatch syncLatch = ongoingSyncs.get(serverId);
    syncLatch.countDown();
  }

  /**
   * Registers a worker ({@code workerId}) to be notified about updates in the routing table.
   * As a result of this method, it sends the PS server-side EM's routing table to the new {code ParameterWorker}
   * if all the initial servers have been registered (i.e., {@code initialServersReady} is true for server-side EM).
   * Otherwise it puts the worker id into a queue, so the worker will be notified
   * when {@link #registerServer(int, String)} is called by all the initial servers.
   * @param workerId Id of worker to be notified updates in routing table.
   */
  synchronized void registerWorker(final String workerId) {
    if (activeWorkerIds.isEmpty()) {
      serverEM.registerRoutingTableUpdateCallback(CLIENT_ID, new EMRoutingTableUpdateHandler());
    }
    activeWorkerIds.add(workerId);

    // wait for all servers to be registered
    if (!initialServersReady) {
      waitingWorkers.add(workerId);

    } else {
      final EMRoutingTable routingTable = new EMRoutingTable(serverEM.getStoreIdToBlockIds(), storeIdToEndpointId);
      sendWorkerRegisterReplyMsg(workerId, routingTable);
    }
  }

  /**
   * Deregisters a worker, {@code workerId} when the worker stops working.
   * After invoking this method, the worker will not be notified with the further update of the routing table.
   * @param workerId Id of worker, who will not subscribe the notification any more.
   */
  synchronized void deregisterWorker(final String workerId) {
    activeWorkerIds.remove(workerId);
    if (activeWorkerIds.isEmpty()) {
      serverEM.deregisterRoutingTableUpdateCallback(CLIENT_ID);
    }
  }

  private void sendWorkerRegisterReplyMsg(final String workerId, final EMRoutingTable routingTable) {
    final List<IdMapping> idMappings = new ArrayList<>(routingTable.getStoreIdToEndpointId().size());
    final Map<Integer, String> storeIdToEndpointIdMap = routingTable.getStoreIdToEndpointId();

    for (final Map.Entry<Integer, Set<Integer>> entry : routingTable.getStoreIdToBlockIds().entrySet()) {
      final int storeId = entry.getKey();
      final List<Integer> blockIds = new ArrayList<>(entry.getValue());

      final IdMapping idMapping = IdMapping.newBuilder()
          .setMemoryStoreId(storeId)
          .setBlockIds(blockIds)
          .setEndpointId(storeIdToEndpointIdMap.get(storeId))
          .build();
      idMappings.add(idMapping);
    }

    final WorkerRegisterReplyMsg workerRegisterReplyMsg = WorkerRegisterReplyMsg.newBuilder()
        .setIdMappings(idMappings)
        .build();

    final AvroPSMsg responseMsg =
        AvroPSMsg.newBuilder()
            .setType(Type.WorkerRegisterReplyMsg)
            .setWorkerRegisterReplyMsg(workerRegisterReplyMsg)
            .build();

    sender.get().send(workerId, responseMsg);
  }

  /**
   * Broadcasts update in routing tables of Server-side EM to all active {@code ParameterWorker}s.
   */
  private synchronized void broadcastMsg(final AvroPSMsg updateMsg) {
    for (final String workerId : activeWorkerIds) {
      sender.get().send(workerId, updateMsg);
    }
  }

  /**
   * A handler of EMRoutingTableUpdate.
   * It broadcasts the update information to all active {@code ParameterWorker}s.
   */
  private final class EMRoutingTableUpdateHandler implements EventHandler<EMRoutingTableUpdate> {
    @Override
    public void onNext(final EMRoutingTableUpdate emRoutingTableUpdate) {
      final int oldOwnerId = emRoutingTableUpdate.getOldOwnerId();
      final int newOwnerId = emRoutingTableUpdate.getNewOwnerId();
      final String newEvalID = emRoutingTableUpdate.getNewEvalId();
      final int blockId = emRoutingTableUpdate.getBlockId();

      final RoutingTableUpdateMsg routingTableUpdateMsg = RoutingTableUpdateMsg.newBuilder()
          .setOldOwnerId(oldOwnerId)
          .setNewOwnerId(newOwnerId)
          .setNewEvalId(newEvalID)
          .setBlockId(blockId)
          .build();

      final AvroPSMsg updateMsg =
          AvroPSMsg.newBuilder()
              .setType(Type.RoutingTableUpdateMsg)
              .setRoutingTableUpdateMsg(routingTableUpdateMsg)
              .build();

      broadcastMsg(updateMsg);
    }
  }
}
