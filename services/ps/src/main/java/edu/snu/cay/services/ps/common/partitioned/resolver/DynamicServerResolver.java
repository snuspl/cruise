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

import edu.snu.cay.services.em.driver.api.RoutingInfo;
import edu.snu.cay.services.ps.worker.partitioned.PartitionedWorkerMsgSender;
import org.apache.reef.tang.InjectionFuture;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import static edu.snu.cay.services.ps.common.Constants.SERVER_ID_PREFIX;

/**
 * Resolves the server based on Elastic Memory's ownership table. This implementation assumes that Elastic Memory
 * locates the data as follows:
 *    If h is the hashed value of the key, h is stored at block b where b's id = h / BLOCK_SIZE.
 */
public class DynamicServerResolver implements ServerResolver {
  private static final Logger LOG = Logger.getLogger(DynamicServerResolver.class.getName());
  private static final long INITIALIZATION_TIMEOUT_MS = 3000;

  private final InjectionFuture<PartitionedWorkerMsgSender> sender;

  private final Map<Integer, Integer> blockToServer = new HashMap<>();

  private long blockSize = 0;

  private volatile boolean initialized = false;

  @Inject
  private DynamicServerResolver(final InjectionFuture<PartitionedWorkerMsgSender> sender) {
    this.sender = sender;
  }

  @Override
  public String resolveServer(final int hash) {
    try {
      if (!initialized) {
        synchronized (this) {
          sender.get().sendRoutingTableRequestMsg();
          this.wait(INITIALIZATION_TIMEOUT_MS);
        }
      }
    } catch (final InterruptedException e) {
      throw new RuntimeException("Failed to initialize routing table for resolving server", e);
    }

    final int blockId = (int) (hash / blockSize);
    return SERVER_ID_PREFIX + blockToServer.get(blockId);
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
  @SuppressWarnings("unchecked")
  @Override
  public void updateRoutingTable(final RoutingInfo routingInfo) {
    final Map<Integer, Integer> routingTable = routingInfo.getBlockIdToStoreId();
    this.blockSize = routingInfo.getBlockSize();
    this.blockToServer.putAll(routingTable);
    initialized = true;
    synchronized (this) {
      notify();
    }
  }
}
