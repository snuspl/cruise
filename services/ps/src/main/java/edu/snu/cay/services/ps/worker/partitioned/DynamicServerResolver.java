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
package edu.snu.cay.services.ps.worker.partitioned;

import edu.snu.cay.services.ps.common.partitioned.resolver.ServerResolver;
import org.apache.reef.tang.annotations.Unit;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Resolves the server based on Elastic Memory's ownership table. This implementation assumes that Elastic Memory
 * locates the data as follows:
 *    If h is the hashed value of the key, h is stored at block b where b's id = h / BLOCK_SIZE.
 */
@Unit
public class DynamicServerResolver implements ServerResolver {
  private static final long INITIALIZATION_TIMEOUT_MS = 3000;
  private final Map<Integer, String> blockToServer = new HashMap<>();

  private volatile boolean initialized = false;

  @Inject
  private DynamicServerResolver() {
  }

  @Override
  public String resolveServer(final int hash) {
    try {
      synchronized (this) {
        if (!initialized) {
          // initRouter();
          this.wait(INITIALIZATION_TIMEOUT_MS);
        }
      }
    } catch (final InterruptedException e) {
      throw new RuntimeException("Failed to initialize routing table for resolving server", e);
    }

    final int blockId = getBlockId(hash);
    return blockToServer.get(blockId);
  }

  private int getBlockId(final int hash) {
    return -1;
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
  private void initRouter() {
    // deserialize.
    // init router
    initialized = true;
  }
}
