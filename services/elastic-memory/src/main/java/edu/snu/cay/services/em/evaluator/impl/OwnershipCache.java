/*
 * Copyright (C) 2017 Seoul National University
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
import edu.snu.cay.services.em.common.parameters.NumInitialEvals;
import edu.snu.cay.services.em.common.parameters.NumTotalBlocks;
import edu.snu.cay.services.em.msg.api.EMMsgSender;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.Optional;
import org.htrace.Trace;
import org.htrace.TraceInfo;
import org.htrace.TraceScope;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 */
public final class OwnershipCache {
  private static final Logger LOG = Logger.getLogger(OwnershipCache.class.getName());

  private static final long INIT_WAIT_TIMEOUT_MS = 5000;
  private static final int MAX_NUM_INIT_REQUESTS = 3;

  private final InjectionFuture<EMMsgSender> msgSender;

  /**
   * A latch that opens when initialization is done.
   */
  private final CountDownLatch initLatch = new CountDownLatch(1);

  private final int localStoreId;

  /**
   * The number of total blocks.
   */
  private final int numTotalBlocks;

  /**
   * The number of initial Evaluators.
   */
  private final int numInitialEvals;

  /**
   * Array representing block locations.
   * Its index is the blockId and value is the storeId.
   */
  private final AtomicIntegerArray blockLocations;
  private final List<Integer> initialLocalBlocks;

  private final ReadWriteLock ownershipLock = new ReentrantReadWriteLock(true);
  private final Map<Integer, CountDownLatch> migratingBlocks = Collections.synchronizedMap(new HashMap<>());

  @Inject
  private OwnershipCache(
      final InjectionFuture<EMMsgSender> msgSender,
      @Parameter(NumTotalBlocks.class) final int numTotalBlocks,
      @Parameter(MemoryStoreId.class) final int memoryStoreId,
      @Parameter(NumInitialEvals.class) final int numInitialEvals) {
    this.msgSender = msgSender;
    this.localStoreId = memoryStoreId;
    this.numTotalBlocks = numTotalBlocks;
    this.numInitialEvals = numInitialEvals;

    this.blockLocations = new AtomicIntegerArray(numTotalBlocks);
    this.initialLocalBlocks = Collections.emptyList();
  }

  /**
   * Requests a routing table to driver.
   */
  private void requestRoutingTable() {
    LOG.log(Level.FINE, "Sends a request for the routing table");
    try (TraceScope traceScope = Trace.startSpan("ROUTING_TABLE_REQUEST")) {
      final TraceInfo traceInfo = TraceInfo.fromSpan(traceScope.getSpan());
      msgSender.get().sendRoutingTableInitReqMsg(traceInfo);
    }
  }

  /**
   * Initializes the routing table with the info received from the driver.
   * This method is only for evaluators added by EM.add(),
   * whose routing table should be initiated from the existing information.
   * It'd be invoked by the network response of {@link #requestRoutingTable()}.
   */
  public synchronized void initOwnershipInfo(final List<Integer> initBlockLocations) {
    if (initLatch.getCount() == 0) {
      return;
    }

    if (initBlockLocations.size() != numTotalBlocks) {
      throw new RuntimeException("Imperfect routing table");
    }

    for (int blockId = 0; blockId < numTotalBlocks; blockId++) {
      final int storeId = initBlockLocations.get(blockId);
      this.blockLocations.set(blockId, storeId);
    }

    initLatch.countDown();
  }

  /**
   * Triggers initialization by requesting initial routing table to driver and waits within a bounded time.
   * It throws RuntimeException, if the table is not initialized til the end.
   * For evaluators not added by EM, it does not trigger initialization.
   * @return a future of initialization thread, a completed future for evaluators not added by EM
   */
  public Future triggerInitialization() {
    return Executors.newSingleThreadExecutor().submit(() -> {
      // sends init request and waits for several times
      for (int reqCount = 0; reqCount < MAX_NUM_INIT_REQUESTS; reqCount++) {
        requestRoutingTable();

        LOG.log(Level.INFO, "Waiting {0} ms for router to be initialized", INIT_WAIT_TIMEOUT_MS);
        try {
          if (initLatch.await(INIT_WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            LOG.log(Level.INFO, "Operation router is initialized");
            return;
          }
        } catch (final InterruptedException e) {
          LOG.log(Level.WARNING, "Interrupted while waiting for router to be initialized", e);
        }
      }
      throw new RuntimeException("Fail to initialize the router");
    });
  }

  public boolean isInitialized() {
    return initLatch.getCount() == 0;
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
   * Resolves an evaluator id for a block id.
   * Be aware that the result of this method might become wrong by {@link #updateOwnership}.
   * @param blockId an id of block
   * @return a Tuple of an Optional with an evaluator id, which is empty when the block belong to the local MemoryStore
   */
  public Optional<Integer> resolveStore(final int blockId) {
    checkInitialization();

    final int memoryStoreId = blockLocations.get(blockId);
    return Optional.of(memoryStoreId);
  }

  /**
   * Resolves an evaluator id for a block id.
   * Be aware that the result of this method might become wrong by {@link #updateOwnership}.
   * @param blockId an id of block
   * @return a Tuple of an Optional with an evaluator id, which is empty when the block belong to the local MemoryStore
   */
  public Tuple<Optional<Integer>, Lock> resolveStoreWithLock(final int blockId) {
    checkInitialization();

    final Lock readLock = ownershipLock.readLock();
    readLock.lock();

    final int memoryStoreId = blockLocations.get(blockId);
    return new Tuple<>(Optional.of(memoryStoreId), readLock);
  }

  /**
   * @return a list of block ids which are initially assigned to the local MemoryStore.
   */
  public List<Integer> getInitialLocalBlockIds() {
    return Collections.unmodifiableList(initialLocalBlocks);
  }

  /**
   * @return a list of block ids which are currently assigned to the local MemoryStore.
   */
  public List<Integer> getCurrentLocalBlockIds() {
    checkInitialization();

    final List<Integer> localBlockIds = new ArrayList<>();
    for (int blockId = 0; blockId < blockLocations.length(); blockId++) {
      final int storeId = blockLocations.get(blockId);
      if (storeId == localStoreId) {
        localBlockIds.add(blockId);
      }
    }
    return localBlockIds;
  }

  /**
   * Updates the owner of the block. Note that this method must be synchronized
   * to prevent other threads from reading the routing information while updating it.
   * @param blockId id of the block to update its ownership.
   * @param oldOwnerId id of the MemoryStore that was owner.
   * @param newOwnerId id of the MemoryStore that will be new owner.
   */
  public void updateOwnership(final int blockId, final int oldOwnerId, final int newOwnerId) {
    checkInitialization();

    ownershipLock.writeLock().lock();
    try {
      final int localOldOwnerId = blockLocations.getAndSet(blockId, newOwnerId);
      if (localOldOwnerId != oldOwnerId) {
        LOG.log(Level.WARNING, "Local routing table thought block {0} was in store {1}, but it was actually in {2}",
            new Object[]{blockId, oldOwnerId, newOwnerId});
      }
      LOG.log(Level.FINE, "Ownership of block {0} is updated from store {1} to store {2}",
          new Object[]{blockId, oldOwnerId, newOwnerId});
    } finally {
      ownershipLock.writeLock().unlock();
    }
  }

  /**
   * Mark a block as migrating and stop client's access on the migrating block.
   * @param blockId id of the block
   */
  void markBlockAsMigrating(final int blockId) {
    synchronized (migratingBlocks) {
      if (migratingBlocks.containsKey(blockId)) {
        throw new RuntimeException("Block" + blockId + " is already in migrating state");
      }

      migratingBlocks.put(blockId, new CountDownLatch(1));
    }
  }

  /**
   * Release the block that was marked by {@link #markBlockAsMigrating(int)}
   * and allow clients access the migrated block, which can be either in local or remote MemoryStore.
   * @param blockId id of the block
   */
  void releaseMigratedBlock(final int blockId) {
    synchronized (migratingBlocks) {
      if (!migratingBlocks.containsKey(blockId)) {
        throw new RuntimeException("Block " + blockId + " is not in migrating state");
      }

      final CountDownLatch blockMigratingLatch = migratingBlocks.remove(blockId);
      blockMigratingLatch.countDown();
    }
  }
}
