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

import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Resolves the server based on Elastic Memory's ownership table. This implementation assumes that Elastic Memory
 * locates the data as follows:
 *    If h is the hashed value of the key, h is stored at block b where b's id = h / BLOCK_SIZE.
 */
public final class DynamicServerResolver implements ServerResolver {
  private static final Logger LOG = Logger.getLogger(DynamicServerResolver.class.getName());
  private static final long INITIALIZATION_TIMEOUT_MS = 3000;

  /**
   * Mapping from Block ID to the MemoryStore ID, which is used to resolve the block to MemoryStores.
   */
  private final Map<Integer, Integer> blockIdToStoreId = new HashMap<>();

  /**
   * Mapping from EM's MemoryStore ID to the PS's NCS endpoint ID.
   * This mapping rarely changes compared to the blockIdToStoreId.
   */
  private final Map<Integer, String> storeIdToEndpointId = new HashMap<>();

  private int numTotalBlocks = 0;

  private volatile boolean initialized = false;

  @Inject
  private DynamicServerResolver() {
  }

  @Override
  public String resolveServer(final int hash) {
    try {
      if (!initialized) {
        synchronized (this) {
          this.wait(INITIALIZATION_TIMEOUT_MS);
        }
      }
    } catch (final InterruptedException e) {
      throw new RuntimeException("Failed to initialize routing table for resolving server", e);
    }

    final int blockId = hash % numTotalBlocks;
    return storeIdToEndpointId.get(blockIdToStoreId.get(blockId));
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
      this.notify();
    }
  }

  @Override
  public void updateRoutingTable(final EMRoutingTableUpdate routingTableUpdate) {
    final int oldOwnerId = routingTableUpdate.getOldOwnerId();
    final int newOwnerId = routingTableUpdate.getNewOwnerId();
    for (final int blockId : routingTableUpdate.getBlockIds()) {
      final int actualOldOwnerId = blockIdToStoreId.put(blockId, newOwnerId);
      if (oldOwnerId != actualOldOwnerId) {
        LOG.log(Level.FINER, "Mapping was stale about block {0}", blockId);
      }
    }
    LOG.log(Level.FINE, "Mapping table in server resolver is updated");
  }
}
