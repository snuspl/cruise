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

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import edu.snu.cay.services.em.driver.api.EMRoutingTableUpdate;
import edu.snu.cay.services.em.driver.api.EMMaster;
import edu.snu.cay.services.em.driver.impl.EMRoutingTableUpdateImpl;
import edu.snu.cay.services.ps.avro.*;
import edu.snu.cay.services.ps.common.parameters.NumServers;
import edu.snu.cay.utils.trace.HTraceUtils;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.htrace.Span;
import org.htrace.Trace;
import org.htrace.TraceInfo;
import org.htrace.TraceScope;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
  private final BiMap<Integer, String> storeIdToEndpointId = HashBiMap.create();;

  /**
   * Server-side EMMaster instance.
   */
  private final EMMaster serverEM;

  private final InjectionFuture<PSMessageSender> sender;

  /**
   * Workers participating in the system and subscribing the routing table of servers.
   */
  private final Set<String> activeWorkerIds = new HashSet<>();

  /**
   * Workers waiting for the server initialization to receive the initial routing table for resolving servers.
   */
  private final Set<String> waitingWorkers = new HashSet<>();

  /**
   * Ongoing sync operations that assures workers to be ready for the deletion of a certain server.
   * Key is a id of server to be deleted, and value is a tuple comprised of a set of worker ids to be checked
   * and a callback to notify when the sync operation is completed.
   */
  private final ConcurrentMap<String, Tuple<Set<String>, EventHandler<Void>>> ongoingSyncs = new ConcurrentHashMap<>();

  /**
   * The number of initial servers, which is for deciding when to send the routing table to workers at the beginning.
   */
  private final int numInitServers;

  /**
   * A boolean flag representing whether all the initial servers are registered.
   */
  private volatile boolean initialServersReady = false;

  @Inject
  EMRoutingTableManager(final EMMaster serverEM,
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
   * Called when a Server instance based on EM is deleted.
   * This method cleans up existing metadata for the deleted server.
   * @param endpointId The Endpoint id in PS.
   */
  public synchronized void deregisterServer(final String endpointId) {
    storeIdToEndpointId.inverse().remove(endpointId);
  }

  /**
   * Checks whether all workers are ready for server delete.
   * To check the state of workers, it sends a sync message to all active workers
   * and the workers will reply when they become ready.
   * On receiving reply from all workers that messages are sent to, a callback will be invoked.
   * See {@link #handleWorkerResponseForSync(String, Tuple)}.
   * @param serverId a server id
   * @param callback a callback to be invoked when sync is completed
   */
  public synchronized void checkWorkersTobeReadyForServerDelete(final String serverId,
                                                                final EventHandler<Void> callback) {
    if (activeWorkerIds.isEmpty()) {
      LOG.log(Level.SEVERE, "No existing workers");
      callback.onNext(null); // TODO #746: consider failure of sync operation
      return;
    }

    if (ongoingSyncs.containsKey(serverId)) {
      LOG.log(Level.WARNING, "Sync for {0} is already ongoing", serverId);
      return;
    }

    final Set<String> workersToCheck = new HashSet<>(activeWorkerIds);
    ongoingSyncs.put(serverId, new Tuple<>(workersToCheck, callback));

    LOG.log(Level.INFO, "Send sync msg for {0} to workers: {1}", new Object[]{serverId, workersToCheck});
    broadcastSyncMsg(serverId, workersToCheck);
  }

  private void broadcastSyncMsg(final String serverId, final Set<String> workersToCheck) {
    final RoutingTableSyncMsg routingTableSyncMsg = RoutingTableSyncMsg.newBuilder()
        .setServerId(serverId)
        .build();

    final AvroPSMsg syncMsg =
        AvroPSMsg.newBuilder()
            .setType(Type.RoutingTableSyncMsg)
            .setRoutingTableSyncMsg(routingTableSyncMsg)
            .build();

    for (final String workerId : workersToCheck) {
      sender.get().send(workerId, syncMsg);
    }
  }

  synchronized void onSyncReply(final String serverId, final String workerId) {
    if (!ongoingSyncs.containsKey(serverId)) {
      LOG.log(Level.WARNING, "Sync for {0} is already completed", serverId);
      return;
    }

    LOG.log(Level.INFO, "Sync reply for {0} has been arrived from worker {1}", new Object[]{serverId, workerId});

    final Tuple<Set<String>, EventHandler<Void>> workerSetAndCallback = ongoingSyncs.get(serverId);
    if (handleWorkerResponseForSync(workerId, workerSetAndCallback)) {
      ongoingSyncs.remove(serverId);
    }
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

    // check all ongoing syncs
    for (final Map.Entry<String, Tuple<Set<String>, EventHandler<Void>>> entry : ongoingSyncs.entrySet()) {
      final Tuple<Set<String>, EventHandler<Void>> workerSetAndCallback = entry.getValue();

      if (handleWorkerResponseForSync(workerId, workerSetAndCallback)) {
        final String serverId = entry.getKey();
        ongoingSyncs.remove(serverId);
      }
    }
  }

  /**
   * Handle workers's responses for sync operation.
   * It is invoked when a worker sends a sync reply msg (see {@link #onSyncReply(String, String)})
   * or the worker is deleted (see {@link #deregisterWorker(String)}).
   * We do not care {@link #registerWorker(String)} method,
   * because newly started workers receive up-to-date routing table.
   * It returns true if sync has been completed by this response.
   */
  private boolean handleWorkerResponseForSync(final String workerId,
                                              final Tuple<Set<String>, EventHandler<Void>> workerSetAndCallback) {
    final Set<String> workersToCheck = workerSetAndCallback.getKey();
    workersToCheck.remove(workerId);

    // invoke callback, when all workers have replied
    if (workersToCheck.isEmpty()) {
      final EventHandler<Void> callback = workerSetAndCallback.getValue();
      callback.onNext(null);
      return true;
    }
    return false;
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
  private synchronized void broadcastMsg(final RoutingTableUpdateMsg routingTableUpdateMsg,
                                         final TraceInfo parentTraceInfo) {
    for (final String workerId : activeWorkerIds) {
      Span detached = null;

      try (TraceScope traceScope = Trace.startSpan("send_routing_table_update_msg. worker_id: " + workerId,
          parentTraceInfo)) {

        detached = traceScope.detach();
        final TraceInfo traceInfo = TraceInfo.fromSpan(detached);

        final AvroPSMsg updateMsg =
            AvroPSMsg.newBuilder()
                .setType(Type.RoutingTableUpdateMsg)
                .setRoutingTableUpdateMsg(routingTableUpdateMsg)
                .setTraceInfo(HTraceUtils.toAvro(traceInfo))
                .build();

        sender.get().send(workerId, updateMsg);

      } finally {
        Trace.continueSpan(detached).close();
      }
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
      final TraceInfo parentTraceInfo = ((EMRoutingTableUpdateImpl) emRoutingTableUpdate).getTraceInfo();

      Trace.setProcessId(EMRoutingTableManager.class.getSimpleName());

      final RoutingTableUpdateMsg routingTableUpdateMsg = RoutingTableUpdateMsg.newBuilder()
          .setOldOwnerId(oldOwnerId)
          .setNewOwnerId(newOwnerId)
          .setNewEvalId(newEvalID)
          .setBlockId(blockId)
          .build();

      broadcastMsg(routingTableUpdateMsg, parentTraceInfo);
    }
  }
}
