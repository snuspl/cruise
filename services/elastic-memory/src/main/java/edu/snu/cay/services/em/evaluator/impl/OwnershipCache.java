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
import edu.snu.cay.services.em.msg.api.EMMsgSender;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.io.Tuple;
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
 * OwnershipCache that maintains ownership info, which is a mapping between blocks and owning evaluators.
 * In addition, it locks and unlocks block upon migration, which should be excluded from block access.
 */
@Private
@NotThreadSafe
public final class OwnershipCache {
  private static final Logger LOG = Logger.getLogger(OwnershipCache.class.getName());

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

  private final InjectionFuture<EMMsgSender> msgSender;

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

  private final Map<Integer, CountDownLatch> incomingBlocks = new ConcurrentHashMap<>();
  private final Map<Integer, ReadWriteLock> ownershipLocks = new HashMap<>();

  @Inject
  private OwnershipCache(final InjectionFuture<EMMsgSender> msgSender,
                         @Parameter(NumTotalBlocks.class) final int numTotalBlocks,
                         @Parameter(NumInitialEvals.class) final int numInitialEvals,
                         @Parameter(MemoryStoreId.class) final int memoryStoreId,
                         @Parameter(AddedEval.class) final boolean addedEval) {
    this.msgSender = msgSender;
    this.localStoreId = memoryStoreId;
    this.numTotalBlocks = numTotalBlocks;
    this.numInitialEvals = numInitialEvals;
    this.addedEval = addedEval;
    this.blockLocations = new AtomicIntegerArray(numTotalBlocks);

    for (int blockId = 0; blockId < numTotalBlocks; blockId++) {
      ownershipLocks.put(blockId, new ReentrantReadWriteLock(true));
    }

    if (!addedEval) {
      final int numInitialLocalBlocks = numTotalBlocks / numInitialEvals + 1; // +1 for remainders
      this.initialLocalBlocks = new ArrayList<>(numInitialLocalBlocks);
      initWithoutDriver();
    } else {
      this.initialLocalBlocks = Collections.emptyList();
    }
  }

  /**
   * Initializes with its local blocks, which are determined statically.
   * Note that if the MemoryStore is created by EM.add(), this method should not be called
   * because the block location might have been updated by EM.move() calls before this add() is called.
   */
  private void initWithoutDriver() {
    // initial evaluators can initialize the ownership cache by itself
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
    LOG.log(Level.INFO, "Initialize ownership cache with localEndPointId: {0}", endpointId);
  }

  /**
   * Requests an ownership info to driver.
   */
  private void requestOwnershipInfo() {
    LOG.log(Level.FINE, "Sends a request for the ownership info");
    try (TraceScope traceScope = Trace.startSpan("OWNERSHIP_INFO_REQUEST")) {
      final TraceInfo traceInfo = TraceInfo.fromSpan(traceScope.getSpan());
      msgSender.get().sendOwnershipCacheInitReqMsg(traceInfo);
    }
  }

  /**
   * Initializes the ownership cache with the info received from the driver.
   * This method is only for evaluators added by EM.add(),
   * whose ownership cache should be initiated from the existing information.
   * It'd be invoked by the network response of {@link #requestOwnershipInfo()}.
   */
  public synchronized void initWithDriver(final List<Integer> initBlockLocations) {
    if (!addedEval || initLatch.getCount() == 0) {
      return;
    }

    if (initBlockLocations.size() != numTotalBlocks) {
      throw new RuntimeException("Imperfect ownership info");
    }

    for (int blockId = 0; blockId < numTotalBlocks; blockId++) {
      final int storeId = initBlockLocations.get(blockId);

      // the evaluators initiated though this evaluators should not have any stores at the beginning
      if (storeId == localStoreId) {
        throw new RuntimeException("Wrong initial ownership info");
      }
      this.blockLocations.set(blockId, storeId);
    }

    initLatch.countDown();
  }

  /**
   * Checks the initialization of the ownership cache.
   * It returns if the ownership cache has been initialized,
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
        LOG.log(Level.WARNING, "Interrupted while waiting for initialization by driver", e);
      }
    }
  }

  /**
   * Triggers initialization by requesting initial ownership info to driver and waits within a bounded time.
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
          requestOwnershipInfo();

          LOG.log(Level.INFO, "Waiting {0} ms for ownership cache to be initialized", INIT_WAIT_TIMEOUT_MS);
          try {
            if (initLatch.await(INIT_WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
              LOG.log(Level.INFO, "Ownership cache is initialized");
              return;
            }
          } catch (final InterruptedException e) {
            LOG.log(Level.WARNING, "Interrupted while waiting for ownership cache to be initialized", e);
          }
        }
        throw new RuntimeException("Fail to initialize the ownership cache");
      }
    });
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
   * Note that this method guarantees that the state of ownership cache does not change
   * before an user unlocks the returned lock.
   * @param blockId an id of block
   * @return a Tuple of an Optional with an evaluator id, which is empty when the block belong to the local MemoryStore,
   *        and a lock that prevents updates to ownership cache
   */
  public Tuple<Optional<String>, Lock> resolveEvalWithLock(final int blockId) {
    checkInitialization();

    final Lock readLock = ownershipLocks.get(blockId).readLock();
    readLock.lock();

    // it should be done while holding a read-lock
    waitBlockMigrationToEnd(blockId);

    final int memoryStoreId = blockLocations.get(blockId);
    if (memoryStoreId == localStoreId) {
      return new Tuple<>(Optional.empty(), readLock);
    } else {
      return new Tuple<>(Optional.of(getEvalId(memoryStoreId)), readLock);
    }
  }

  /**
   * Wait until {@link #allowAccessToBlock} is called for a block if it's blocked by {@link #blockAccessToBlock}.
   * @param blockId an id of the block
   */
  private void waitBlockMigrationToEnd(final int blockId) {
    final CountDownLatch blockMigratingLatch = incomingBlocks.get(blockId);
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
   * Updates the owner of the block.
   * This method takes a exclusive lock on a block against {@link #resolveEvalWithLock(int)}
   * to prevent other threads from reading the ownership information while updating it.
   * In addition, in receiver evaluators, it makes {@link #resolveEvalWithLock} wait
   * until {@link #allowAccessToBlock} for the block to access is called.
   * @param blockId id of the block to update its ownership.
   * @param oldOwnerId id of the MemoryStore that was owner.
   * @param newOwnerId id of the MemoryStore that will be new owner.
   */
  public void updateOwnership(final int blockId, final int oldOwnerId, final int newOwnerId) {
    checkInitialization();

    ownershipLocks.get(blockId).writeLock().lock();
    try {
      final int localOldOwnerId = blockLocations.getAndSet(blockId, newOwnerId);
      if (localOldOwnerId != oldOwnerId) {
        LOG.log(Level.WARNING, "Local ownership cache thought block {0} was in store {1}, but it was actually in {2}",
            new Object[]{blockId, oldOwnerId, newOwnerId});
      }
      LOG.log(Level.FINE, "Ownership of block {0} is updated from store {1} to store {2}",
          new Object[]{blockId, oldOwnerId, newOwnerId});

      if (localStoreId == newOwnerId) {
        // it should be done while holding a write-lock
        blockAccessToBlock(blockId);
      }
    } finally {
      ownershipLocks.get(blockId).writeLock().unlock();
    }
  }

  /**
   * Blocks access to a block until {@link #allowAccessToBlock} is called.
   * @param blockId if of the block
   */
  private void blockAccessToBlock(final int blockId) {
    incomingBlocks.put(blockId, new CountDownLatch(1));
  }

  /**
   * Allows access to a block when it completely migrates into local store.
   * @param blockId id of the block
   */
  public void allowAccessToBlock(final int blockId) {
    if (!incomingBlocks.containsKey(blockId)) {
      throw new RuntimeException("Block " + blockId + " is not in migrating state");
    }

    final CountDownLatch blockMigratingLatch = incomingBlocks.remove(blockId);
    blockMigratingLatch.countDown();
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
