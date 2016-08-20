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
import edu.snu.cay.services.ps.worker.impl.WorkerMsgSender;
import org.apache.reef.tang.InjectionFuture;

import javax.inject.Inject;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Resolves the server based on Elastic Memory's ownership table. This implementation assumes that Elastic Memory
 * locates the data as follows:
 *    If h is the hashed value of the key, h is stored at block b where b's id = h / BLOCK_SIZE.
 */
public final class DynamicServerResolver implements ServerResolver {
  private static final Logger LOG = Logger.getLogger(DynamicServerResolver.class.getName());

  private static final long INIT_WAIT_TIMEOUT_MS = 2000;
  private static final int MAX_NUM_INIT_REQUESTS = 3;

  /**
   * Mapping from Block ID to the MemoryStore ID, which is used to resolve the block to MemoryStores.
   */
  private final Map<Integer, Integer> blockIdToStoreId = new ConcurrentHashMap<>();

  /**
   * Mapping from EM's MemoryStore ID to the PS's NCS endpoint ID.
   * This mapping rarely changes compared to the blockIdToStoreId.
   */
  private final Map<Integer, String> storeIdToEndpointId = new ConcurrentHashMap<>();

  private volatile int numTotalBlocks = 0;

  /**
   * A latch that opens when initialization is done.
   */
  private final CountDownLatch initLatch = new CountDownLatch(1);

  /**
   * A set maintaining sync requests about the deletion of a certain server.
   */
  private final Set<String> ongoingSyncs = Collections.newSetFromMap(new ConcurrentHashMap<>());

  private final InjectionFuture<WorkerMsgSender> msgSender;

  @Inject
  private DynamicServerResolver(final InjectionFuture<WorkerMsgSender> msgSender) {
    this.msgSender = msgSender;
  }

  @Override
  public String resolveServer(final int hash) {
    checkInitialization();

    final int blockId = hash % numTotalBlocks;
    final int storeId = blockIdToStoreId.get(blockId);
    return storeIdToEndpointId.get(storeId);
  }

  /**
   * Checks the initialization of the routing table.
   * It returns if the routing table has been initialized,
   * otherwise waits the initialization within a bounded time.
   */
  private void checkInitialization() {
    while (true) {
      try {
        initLatch.await();
        break;
      } catch (final InterruptedException e) {
        LOG.log(Level.WARNING, "Interrupted while waiting for routing table initialization from driver", e);
      }
    }
  }

  /**
   * Triggers initialization by requesting initial routing table to driver and waits within a bounded time.
   * It throws RuntimeException, if the table is not initialized til the end.
   * Since initialization takes time, it executes initialization asynchronously.
   * @return a future of initialization thread
   */
  public Future triggerInitialization() {
    return Executors.newSingleThreadExecutor().submit(new Runnable() {
      @Override
      public void run() {
        // sends init request and waits for several times
        for (int reqCount = 0; reqCount < MAX_NUM_INIT_REQUESTS; reqCount++) {
          requestRoutingTable();

          LOG.log(Level.INFO, "Waiting {0} ms for router to be initialized", INIT_WAIT_TIMEOUT_MS);
          try {
            if (initLatch.await(INIT_WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
              return;
            }
          } catch (final InterruptedException e) {
            LOG.log(Level.WARNING, "Interrupted while waiting for router to be initialized", e);
          }
        }
        throw new RuntimeException("Fail to initialize the resolver");
      }
    });
  }

  /**
   * Requests a routing table to driver.
   */
  private void requestRoutingTable() {
    LOG.log(Level.FINE, "Sends a request for the routing table");
    msgSender.get().sendWorkerRegisterMsg();
  }

  @Override
  public int resolvePartition(final int hash) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Integer> getPartitions(final String server) {
    throw new UnsupportedOperationException();
  }

  /**
   * Initialize the router to lookup.
   */
  @Override
  public synchronized void initRoutingTable(final EMRoutingTable routingTable) {
    if (initLatch.getCount() == 0) {
      return;
    }

    final Map<Integer, Set<Integer>> storeIdToBlockIds = routingTable.getStoreIdToBlockIds();

    numTotalBlocks = routingTable.getNumTotalBlocks();
    storeIdToEndpointId.putAll(routingTable.getStoreIdToEndpointId());

    for (final Map.Entry<Integer, Set<Integer>> entry : storeIdToBlockIds.entrySet()) {
      final int storeId = entry.getKey();
      for (final int blockId : entry.getValue()) {
        blockIdToStoreId.put(blockId, storeId);
      }
    }

    initLatch.countDown();

    LOG.log(Level.FINE, "Server resolver is initialized");
    // wake up all waiting threads
    this.notifyAll();
  }

  @Override
  public void updateRoutingTable(final EMRoutingTableUpdate routingTableUpdate) {
    checkInitialization();

    final int oldOwnerId = routingTableUpdate.getOldOwnerId();
    final int newOwnerId = routingTableUpdate.getNewOwnerId();
    final String newEvalId = routingTableUpdate.getNewEvalId();

    // add or replace an eval id of a store
    storeIdToEndpointId.put(newOwnerId, newEvalId);

    final int blockId = routingTableUpdate.getBlockId();

    final int actualOldOwnerId = blockIdToStoreId.put(blockId, newOwnerId);
    if (oldOwnerId != actualOldOwnerId) {
      LOG.log(Level.FINER, "Mapping was stale about block {0}", blockId);
    } else {
      LOG.log(Level.FINE, "Mapping table in server resolver is updated");
    }

    // remove old server eval id, if it has no block
    if (!blockIdToStoreId.containsValue(oldOwnerId)) {
      final String deletedServerId = storeIdToEndpointId.remove(oldOwnerId);

      if (deletedServerId != null && ongoingSyncs.remove(deletedServerId)) {
        msgSender.get().sendRoutingTableSyncReplyMsg(deletedServerId);
      }
    }
  }

  @Override
  public synchronized void syncRoutingTable(final String serverId) {
    if (!storeIdToEndpointId.containsValue(serverId)) {
      msgSender.get().sendRoutingTableSyncReplyMsg(serverId);
    } else {
      ongoingSyncs.add(serverId);
    }
  }
}
