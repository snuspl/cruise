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
import edu.snu.cay.services.ps.worker.partitioned.PartitionedWorkerMsgSender;
import org.apache.reef.tang.InjectionFuture;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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

  private int numTotalBlocks = 0;

  /**
   * Boolean representing whether it receives the initial routing table from driver or not.
   */
  private volatile boolean initialized = false;

  private final InjectionFuture<PartitionedWorkerMsgSender> msgSender;

  @Inject
  private DynamicServerResolver(final InjectionFuture<PartitionedWorkerMsgSender> msgSender) {
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
   * otherwise requests initial routing table to driver and waits within a bounded time.
   * It throws RuntimeException, if the table is not initialized til the end.
   */
  private void checkInitialization() {
    if (initialized) {
      return;
    }

    // sends init request and waits for several times
    for (int reqCount = 0; reqCount < MAX_NUM_INIT_REQUESTS; reqCount++) {
      requestRoutingTable();

      LOG.log(Level.INFO, "Waiting {0} ms for router to be initialized", INIT_WAIT_TIMEOUT_MS);
      try {
        synchronized (this) {
          this.wait(INIT_WAIT_TIMEOUT_MS);
        }
      } catch (final InterruptedException e) {
        LOG.log(Level.WARNING, "Interrupted while waiting for router to be initialized", e);
      }

      if (initialized) {
        return;
      }
    }
    throw new RuntimeException("Fail to initialize the resolver");
  }

  /**
   * Requests a routing table to driver.
   */
  public void requestRoutingTable() {
    if (initialized) {
      return;
    }

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
  public void initRoutingTable(final EMRoutingTable routingTable) {
    final Map<Integer, Set<Integer>> storeIdToBlockIds = routingTable.getStoreIdToBlockIds();

    numTotalBlocks = routingTable.getNumTotalBlocks();
    storeIdToEndpointId.putAll(routingTable.getStoreIdToEndpointId());

    for (final Map.Entry<Integer, Set<Integer>> entry : storeIdToBlockIds.entrySet()) {
      final int storeId = entry.getKey();
      for (final int blockId : entry.getValue()) {
        blockIdToStoreId.put(blockId, storeId);
      }
    }

    initialized = true;
    LOG.log(Level.FINE, "Server resolver is initialized");
    synchronized (this) {
      // wake up all waiting threads
      this.notifyAll();
    }
  }

  @Override
  public void updateRoutingTable(final EMRoutingTableUpdate routingTableUpdate) {
    checkInitialization();

    final int oldOwnerId = routingTableUpdate.getOldOwnerId();
    final int newOwnerId = routingTableUpdate.getNewOwnerId();
    final String newEvalId = routingTableUpdate.getNewEvalId();

    // add or replace an eval id of a store
    storeIdToEndpointId.put(newOwnerId, newEvalId);

    for (final int blockId : routingTableUpdate.getBlockIds()) {
      final int actualOldOwnerId = blockIdToStoreId.put(blockId, newOwnerId);
      if (oldOwnerId != actualOldOwnerId) {
        LOG.log(Level.FINER, "Mapping was stale about block {0}", blockId);
      }
    }
    LOG.log(Level.FINE, "Mapping table in server resolver is updated");
  }
}
