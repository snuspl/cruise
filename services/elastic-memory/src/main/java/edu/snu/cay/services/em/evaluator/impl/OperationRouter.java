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

import edu.snu.cay.services.em.common.parameters.AddedEval;
import edu.snu.cay.services.em.common.parameters.MemoryStoreId;
import edu.snu.cay.services.em.common.parameters.NumTotalBlocks;
import edu.snu.cay.services.em.common.parameters.NumInitialEvals;
import edu.snu.cay.services.em.evaluator.api.BlockResolver;
import edu.snu.cay.services.em.msg.api.ElasticMemoryMsgSender;
import edu.snu.cay.utils.Tuple3;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.Optional;
import org.htrace.Trace;
import org.htrace.TraceInfo;
import org.htrace.TraceScope;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
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

  private static final long INIT_WAIT_TIMEOUT_MS = 5000;
  private static final int MAX_NUM_INIT_REQUESTS = 3;

  /**
   * A latch that opens when initialization is done.
   */
  private final CountDownLatch initLatch = new CountDownLatch(1);

  /**
   * A boolean representing whether the evaluator is added by EM.add().
   */
  private final boolean addedEval;

  /**
   * A prefix of evaluator id will be set by {@link #setEndpointIdPrefix(String)},
   * and used by {@link #getEvalId(int)} to make the complete evaluator id.
   */
  private volatile String evalPrefix;

  private final int localStoreId;

  private final BlockResolver<K> blockResolver;

  private final InjectionFuture<ElasticMemoryMsgSender> msgSender;

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

  private final ReadWriteLock routerLock = new ReentrantReadWriteLock(true);
  private final Map<Integer, CountDownLatch> migratingBlocks = Collections.synchronizedMap(new HashMap<>());

  @Inject
  private OperationRouter(final BlockResolver<K> blockResolver,
                          final InjectionFuture<ElasticMemoryMsgSender> msgSender,
                          @Parameter(NumTotalBlocks.class) final int numTotalBlocks,
                          @Parameter(NumInitialEvals.class) final int numInitialEvals,
                          @Parameter(MemoryStoreId.class) final int memoryStoreId,
                          @Parameter(AddedEval.class) final boolean addedEval) {
    this.blockResolver = blockResolver;
    this.msgSender = msgSender;
    this.localStoreId = memoryStoreId;
    this.numTotalBlocks = numTotalBlocks;
    this.numInitialEvals = numInitialEvals;
    this.addedEval = addedEval;
    this.blockLocations = new AtomicIntegerArray(numTotalBlocks);

    if (!addedEval) {
      final int numInitialLocalBlocks = numTotalBlocks / numInitialEvals + 1; // +1 for remainders
      this.initialLocalBlocks = new ArrayList<>(numInitialLocalBlocks);
      initRoutingTableWithoutDriver();
    } else {
      this.initialLocalBlocks = Collections.emptyList();
    }
  }

  /**
   * Initializes routing table of this MemoryStore with its local blocks, which are determined statically.
   * Note that if the MemoryStore is created by EM.add(), this method should not be called
   * because the block location might have been updated by EM.move() calls before this add() is called.
   */
  private void initRoutingTableWithoutDriver() {
    // initial evaluators can initialize the routing table by itself
    for (int blockId = localStoreId; blockId < numTotalBlocks; blockId += numInitialEvals) {
      initialLocalBlocks.add(blockId);
    }

    // blocks are initially distributed across Evaluators in round-robin.
    for (int blockId = 0; blockId < numTotalBlocks; blockId++) {
      final int storeId = blockId % numInitialEvals;
      blockLocations.set(blockId, storeId);
    }
  }


  /**
   * Sets a prefix of evaluator that will be used to resolve remote evaluators.
   * Note that this method should be invoked before {@link #triggerInitialization()}.
   */
  public void setEndpointIdPrefix(final String endpointId) {
    // TODO #509: Remove assumption on the format of context id
    this.evalPrefix = endpointId.split("-")[0];
    LOG.log(Level.INFO, "Initialize router with localEndPointId: {0}", endpointId);
  }

  /**
   * Requests a routing table to driver.
   */
  private void requestRoutingTable() {
    LOG.log(Level.FINE, "Sends a request for the routing table");
    try (final TraceScope traceScope = Trace.startSpan("ROUTING_TABLE_REQUEST")) {
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
  public synchronized void initRoutingTableWithDriver(final List<Integer> initBlockLocations) {
    if (!addedEval || initLatch.getCount() == 0) {
      return;
    }

    if (initBlockLocations.size() != numTotalBlocks) {
      throw new RuntimeException("Imperfect routing table");
    }

    for (int blockId = 0; blockId < numTotalBlocks; blockId++) {
      final int storeId = initBlockLocations.get(blockId);

      // the evaluators initiated though this evaluators should not have any stores at the beginning
      if (storeId == localStoreId) {
        throw new RuntimeException("Wrong initial routing table");
      }
      this.blockLocations.set(blockId, storeId);
    }

    initLatch.countDown();
  }

  /**
   * Checks the initialization of the routing table.
   * It returns if the routing table has been initialized,
   * otherwise waits the initialization within a bounded time.
   */
  private void checkInitialization() {
    if (!addedEval) {
      return;
    }

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
   * For evaluators not added by EM, it does not trigger initialization.
   * @return a future of initialization thread, a completed future for evaluators not added by EM
   */
  public Future triggerInitialization() {
    if (!addedEval) {
      return CompletableFuture.completedFuture(null);
    }

    return Executors.newSingleThreadExecutor().submit(new Runnable() {
      @Override
      public void run() {
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
      }
    });
  }

  /**
   * Routes the data key range of the operation. Note that this method must be synchronized to prevent other threads
   * from updating the routing information while reading it.
   *
   * This method does not work with Ownership-first migration protocol.
   * @param dataKeyRanges a range of data keys
   * @return a pair of a map between a block id and a corresponding sub key range,
   * and a map between evaluator id and corresponding sub key ranges.
   */
  public Tuple3<Map<Integer, List<Pair<K, K>>>, Map<String, List<Pair<K, K>>>, Lock> route(
      final List<Pair<K, K>> dataKeyRanges) {
    checkInitialization();

    final Map<Integer, List<Pair<K, K>>> localBlockToSubKeyRangesMap = new HashMap<>();
    final Map<String, List<Pair<K, K>>> remoteEvalToSubKeyRangesMap = new HashMap<>();

    final Lock readLock = routerLock.readLock();
    readLock.lock();

    // dataKeyRanges has at least one element
    // In most cases, there are only one range in dataKeyRanges
    for (final Pair<K, K> keyRange : dataKeyRanges) {

      final Map<Integer, Pair<K, K>> blockToSubKeyRangeMap =
          blockResolver.resolveBlocksForOrderedKeys(keyRange.getFirst(), keyRange.getSecond());
      for (final Map.Entry<Integer, Pair<K, K>> blockToSubKeyRange : blockToSubKeyRangeMap.entrySet()) {
        final int blockId = blockToSubKeyRange.getKey();
        final Pair<K, K> minMaxKeyPair = blockToSubKeyRange.getValue();

        final int memoryStoreId = blockLocations.get(blockId);

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
   * Be aware that the result of this method might become wrong by {@link #updateOwnership}.
   * @param blockId an id of block
   * @return a Tuple of an Optional with an evaluator id, which is empty when the block belong to the local MemoryStore
   */
  public Optional<String> resolveEval(final int blockId) {
    checkInitialization();

    final int memoryStoreId = blockLocations.get(blockId);
    if (memoryStoreId == localStoreId) {
      return Optional.empty();
    } else {
      return Optional.of(getEvalId(memoryStoreId));
    }
  }

  /**
   * Resolves an evaluator id for a block id.
   * Note that this method guarantees that the state of routing table does not change
   * before an user unlocks the returned lock.
   * @param blockId an id of block
   * @return a Tuple of an Optional with an evaluator id, which is empty when the block belong to the local MemoryStore,
   *        and a lock that prevents updates to routing table
   */
  public Tuple<Optional<String>, Lock> resolveEvalWithLock(final int blockId) {
    checkInitialization();

    waitBlockMigrationToEnd(blockId);

    final Lock readLock = routerLock.readLock();
    readLock.lock();
    final int memoryStoreId = blockLocations.get(blockId);
    if (memoryStoreId == localStoreId) {
      return new Tuple<>(Optional.empty(), readLock);
    } else {
      return new Tuple<>(Optional.of(getEvalId(memoryStoreId)), readLock);
    }
  }

  private void waitBlockMigrationToEnd(final int blockId) {
    final CountDownLatch blockMigratingLatch = migratingBlocks.get(blockId);
    if (blockMigratingLatch != null) {
      try {
        blockMigratingLatch.await();
      } catch (final InterruptedException e) {
        throw new RuntimeException("Interrupted while waiting for block migration to be finished", e);
      }
    }
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

    routerLock.writeLock().lock();
    try {
      final int localOldOwnerId = blockLocations.getAndSet(blockId, newOwnerId);
      if (localOldOwnerId != oldOwnerId) {
        LOG.log(Level.WARNING, "Local routing table thought block {0} was in store {1}, but it was actually in {2}",
            new Object[]{blockId, oldOwnerId, newOwnerId});
      }
      LOG.log(Level.FINE, "Ownership of block {0} is updated from store {1} to store {2}",
          new Object[]{blockId, oldOwnerId, newOwnerId});
    } finally {
      routerLock.writeLock().unlock();
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
