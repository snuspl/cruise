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
package edu.snu.cay.services.em.evaluator.impl;

import edu.snu.cay.services.em.common.parameters.MemoryStoreId;
import edu.snu.cay.services.em.evaluator.api.BlockResolver;
import edu.snu.cay.utils.Tuple3;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.Optional;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * OperationRouter that redirects incoming operations on specific data ids to corresponding blocks and evaluators.
 * Note that this class is not thread-safe, which means client of this class must synchronize explicitly.
 * @param <K> type of data key
 */
@Private
@NotThreadSafe
public final class OperationRouter<K> {
  private static final Logger LOG = Logger.getLogger(OperationRouter.class.getName());

  /**
   * A latch that opens when initialization is done.
   */
  private final CountDownLatch initLatch = new CountDownLatch(1);

  /**
   * A prefix of evaluator id will be set by {@link #initialize(String)},
   * and used by {@link #getEvalId(int)} to make the complete evaluator id.
   */
  private volatile String evalPrefix;

  private final int localStoreId;

  private final BlockResolver<K> blockResolver;

  private final OwnershipCache ownershipCache;

  @Inject
  private OperationRouter(final BlockResolver<K> blockResolver,
                          final OwnershipCache ownershipCache,
                          @Parameter(MemoryStoreId.class) final int memoryStoreId) {
    this.blockResolver = blockResolver;
    this.ownershipCache = ownershipCache;
    this.localStoreId = memoryStoreId;
  }

  /**
   * Sets a prefix of evaluator that will be used to resolve remote evaluators.
   */
  public void initialize(final String endpointId) {
    // TODO #509: Remove assumption on the format of context id
    this.evalPrefix = endpointId.split("-")[0];
    LOG.log(Level.INFO, "Initialize router with localEndPointId: {0}", endpointId);
    initLatch.countDown();
  }

  /**
   * Checks the initialization of the router.
   * It returns if the router has been initialized,
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
   * Routes the data key range of the operation.
   * Note that this method guarantees that the state of routing table does not change
   * before an user unlocks the returned lock.
   * This method does not work with Ownership-first migration protocol.
   * @param dataKeyRanges a range of data keys
   * @return a tuple of a map between a block id and a corresponding sub key range,
   *        and a map between evaluator id and corresponding sub key ranges,
   *        and a lock that prevents updates to ownership cache
   */
  public Tuple3<Map<Integer, List<Pair<K, K>>>, Map<String, List<Pair<K, K>>>, Lock> route(
      final List<Pair<K, K>> dataKeyRanges) {
    checkInitialization();

    final Map<Integer, List<Pair<K, K>>> localBlockToSubKeyRangesMap = new HashMap<>();
    final Map<String, List<Pair<K, K>>> remoteEvalToSubKeyRangesMap = new HashMap<>();

    // TODO #957: need to clean up. currently it exploits method for single key to acquire a lock
    final Lock readLock = ownershipCache.resolveStoreWithLock(0).getValue();
    readLock.lock();

    // dataKeyRanges has at least one element
    // In most cases, there are only one range in dataKeyRanges
    for (final Pair<K, K> keyRange : dataKeyRanges) {

      final Map<Integer, Pair<K, K>> blockToSubKeyRangeMap =
          blockResolver.resolveBlocksForOrderedKeys(keyRange.getFirst(), keyRange.getSecond());
      for (final Map.Entry<Integer, Pair<K, K>> blockToSubKeyRange : blockToSubKeyRangeMap.entrySet()) {
        final int blockId = blockToSubKeyRange.getKey();
        final Pair<K, K> minMaxKeyPair = blockToSubKeyRange.getValue();

        final int memoryStoreId = ownershipCache.resolveStore(blockId);

        // aggregate sub ranges
        if (memoryStoreId != localStoreId) {
          final String remoteEvalId = getEvalId(memoryStoreId);
          if (!remoteEvalToSubKeyRangesMap.containsKey(remoteEvalId)) {
            remoteEvalToSubKeyRangesMap.put(remoteEvalId, new LinkedList<Pair<K, K>>());
          }
          final List<Pair<K, K>> remoteRangeList = remoteEvalToSubKeyRangesMap.get(remoteEvalId);
          remoteRangeList.add(minMaxKeyPair);
        } else {
          if (!localBlockToSubKeyRangesMap.containsKey(blockId)) {
            localBlockToSubKeyRangesMap.put(blockId, new LinkedList<Pair<K, K>>());
          }
          final List<Pair<K, K>> localRangeList = localBlockToSubKeyRangesMap.get(blockId);
          localRangeList.add(minMaxKeyPair);
        }
      }
    }

    return new Tuple3<>(localBlockToSubKeyRangesMap, remoteEvalToSubKeyRangesMap, readLock);
  }

  /**
   * Resolves an evaluator id for a block id.
   * Be aware that the result of this method might become wrong by change in {@link OwnershipCache}.
   * @param blockId an id of block
   * @return an Optional with an evaluator id, which is empty when the block belong to the local MemoryStore
   */
  public Optional<String> resolveEval(final int blockId) {
    final Integer storeId = ownershipCache.resolveStore(blockId);
    if (storeId == localStoreId) {
      return Optional.empty();
    } else {
      return Optional.of(getEvalId(storeId));
    }
  }

  /**
   * Resolves an evaluator id for a block id.
   * Note that this method guarantees that the state of routing table does not change
   * before an user unlocks the returned lock.
   * @param blockId an id of block
   * @return a Tuple of an Optional with an evaluator id, which is empty when the block belong to the local MemoryStore,
   *        and a lock that prevents updates to ownership cache
   */
  public Tuple<Optional<String>, Lock> resolveEvalWithLock(final int blockId) {
    final Tuple<Integer, Lock> storeIdWithLock = ownershipCache.resolveStoreWithLock(blockId);

    final Integer storeId = storeIdWithLock.getKey();
    if (storeId == localStoreId) {
      return new Tuple<>(Optional.empty(), storeIdWithLock.getValue());
    } else {
      return new Tuple<>(Optional.of(getEvalId(storeId)), storeIdWithLock.getValue());
    }
  }

  /**
   * TODO #509: it assumes that MemoryStore id is assigned by the suffix of context id.
   * Converts the MemoryStore id to the corresponding Evaluator's endpoint id.
   * @param memoryStoreId MemoryStore's identifier
   * @return the endpoint id to access the MemoryStore.
   */
  private String getEvalId(final int memoryStoreId) {
    return evalPrefix + '-' + memoryStoreId;
  }
}
