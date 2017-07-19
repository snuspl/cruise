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
package edu.snu.cay.services.et.evaluator.impl;

import edu.snu.cay.services.et.configuration.parameters.ExecutorIdentifier;
import edu.snu.cay.services.et.configuration.parameters.NumTotalBlocks;
import edu.snu.cay.services.et.configuration.parameters.TableIdentifier;
import edu.snu.cay.services.et.evaluator.api.MessageSender;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * OwnershipCache that maintains ownership info of a table, which is a mapping between blocks and owning executors.
 * In addition, it locks and unlocks block upon migration, which should be excluded from block access.
 * It also provides a feature for synchronizing the ownership status about the unassociation of a certain executor.
 */
@EvaluatorSide
@ThreadSafe
@Private
public final class OwnershipCache {
  private static final Logger LOG = Logger.getLogger(OwnershipCache.class.getName());

  /**
   * Array representing block locations.
   * Its index is the blockId and value is the storeId.
   */
  private final AtomicReferenceArray<String> blockOwnerArray;

  private final Map<String, AtomicInteger> executorIdToNumBlocks = new ConcurrentHashMap<>();

  /**
   * A set of sync requests about the deletion of a certain executor.
   */
  private final Map<String, Long> ongoingSyncs = new ConcurrentHashMap<>();

  /**
   * A map maintaining incoming blocks in receiver evaluator.
   */
  private final Map<Integer, CountDownLatch> incomingBlocks = new ConcurrentHashMap<>();

  /**
   * A map that holds a read-write lock for each block.
   */
  private final Map<Integer, ReadWriteLock> ownershipLocks = new HashMap<>();

  private final String tableId;
  private final String localExecutorId;

  private final InjectionFuture<MessageSender> msgSenderFuture;

  @Inject
  private OwnershipCache(@Parameter(NumTotalBlocks.class) final int numTotalBlocks,
                         @Parameter(TableIdentifier.class) final String tableId,
                         @Parameter(ExecutorIdentifier.class) final String executorId,
                         final InjectionFuture<MessageSender> msgSenderFuture) {
    this.blockOwnerArray = new AtomicReferenceArray<>(numTotalBlocks);
    this.tableId = tableId;
    this.localExecutorId = executorId;
    this.msgSenderFuture = msgSenderFuture;

    for (int blockId = 0; blockId < numTotalBlocks; blockId++) {
      this.ownershipLocks.put(blockId, new ReentrantReadWriteLock(true));
    }
  }

  /**
   * Initialize this ownership cache.
   * This method should be called once before other methods.
   * @param blockOwners ownership mapping
   */
  public void init(final List<String> blockOwners) {
    for (int blockId = 0; blockId < blockOwners.size(); blockId++) {
      final String owner = blockOwners.get(blockId);
      blockOwnerArray.set(blockId, owner);

      if (!executorIdToNumBlocks.containsKey(owner)) {
        executorIdToNumBlocks.put(owner, new AtomicInteger(0));
      }
      executorIdToNumBlocks.get(owner).incrementAndGet();
    }
  }

  /**
   * Resolves an executor id for a block id.
   * Be aware that the result of this method might become wrong by {@link #update}.
   * @param blockId an id of block
   * @return a Tuple of an Optional with an evaluator id, which is empty when the block belong to the local MemoryStore
   */
  public Optional<String> resolveExecutor(final int blockId) {
    final String ownerId = blockOwnerArray.get(blockId);
    if (ownerId.equals(localExecutorId)) {
      return Optional.empty();
    } else {
      return Optional.of(ownerId);
    }
  }

  /**
   * Resolves an executor id for a block id.
   * Note that this method guarantees that the state of ownership cache does not change
   * before an user unlocks the returned lock.
   * @param blockId an id of block
   * @return a Tuple of an Optional with an evaluator id, which is empty when the block belong to the local MemoryStore,
   *        and a lock that prevents updates to ownership cache
   */
  public Pair<Optional<String>, Lock> resolveExecutorWithLock(final int blockId) {
    final Lock readLock = ownershipLocks.get(blockId).readLock();
    readLock.lock();

    // it should be done while holding a read-lock
    waitBlockMigrationToEnd(blockId);

    final String ownerId = blockOwnerArray.get(blockId);
    if (ownerId.equals(localExecutorId)) {
      return Pair.of(Optional.empty(), readLock);
    } else {
      return Pair.of(Optional.of(ownerId), readLock);
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
   * @return a list of block ids which are currently assigned to the local MemoryStore.
   */
  public List<Integer> getCurrentLocalBlockIds() {
    final List<Integer> localBlockIds = new ArrayList<>();
    for (int blockId = 0; blockId < blockOwnerArray.length(); blockId++) {
      final String ownerId = blockOwnerArray.get(blockId);
      if (ownerId.equals(localExecutorId)) {
        localBlockIds.add(blockId);
      }
    }
    return localBlockIds;
  }

  /**
   * Updates the owner of the block.
   * This method takes a exclusive lock on a block against {@link #resolveExecutorWithLock(int)}
   * to prevent other threads from reading the ownership information while updating it.
   * In addition, in receiver evaluators, it invokes {@link #blockAccessToBlock} to
   * make {@link #resolveExecutorWithLock} wait until {@link #allowAccessToBlock} for the block to access is called.
   * @param blockId id of the block to update its ownership.
   * @param oldOwnerId id of the MemoryStore that was owner.
   * @param newOwnerId id of the MemoryStore that will be new owner.
   */
  public void update(final int blockId, final String oldOwnerId, final String newOwnerId) {
    ownershipLocks.get(blockId).writeLock().lock();
    try {
      final String localOldOwnerId = blockOwnerArray.getAndSet(blockId, newOwnerId);
      if (!localOldOwnerId.equals(oldOwnerId)) {
        LOG.log(Level.WARNING, "Local ownership cache thought block {0} was in store {1}, but it was actually in {2}",
            new Object[]{blockId, oldOwnerId, newOwnerId});
      }
      LOG.log(Level.FINE, "Ownership of block {0} is updated from {1} to {2}",
          new Object[]{blockId, oldOwnerId, newOwnerId});

      if (localExecutorId.equals(newOwnerId)) {
        // it should be done while holding a write-lock
        blockAccessToBlock(blockId);
      }
    } finally {
      ownershipLocks.get(blockId).writeLock().unlock();
    }

    // decrement numBlocks in old owner
    final Long syncOpId;
    synchronized (executorIdToNumBlocks) {
      final int remainingBlocks = executorIdToNumBlocks.get(oldOwnerId).decrementAndGet();
      if (remainingBlocks == 0) {
        executorIdToNumBlocks.remove(oldOwnerId);
        syncOpId = ongoingSyncs.remove(oldOwnerId);
      } else {
        syncOpId = null;
      }
    }

    // increment numBlocks in new owner
    executorIdToNumBlocks.compute(newOwnerId, (id, numBlocksCounter) -> {
      if (numBlocksCounter == null) {
        return new AtomicInteger(1);
      } else {
        numBlocksCounter.incrementAndGet();
        return numBlocksCounter;
      }
    });

    // sync complete
    if (syncOpId != null) {
      completeSync(syncOpId, oldOwnerId);
    }
  }

  /**
   * Completes sync by sending a response msg to master.
   */
  private void completeSync(final long opId, final String deletedExecutorId) {
    LOG.log(Level.INFO, "Sync completed. opId: {0}, tableId: {1}, deletedExecutorId: {2}",
        new Object[]{opId, tableId, deletedExecutorId});
    try {
      msgSenderFuture.get().sendOwnershipSyncAckMsg(opId, tableId, deletedExecutorId);
    } catch (NetworkException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Completes all ongoing syncs before removing this table.
   * Otherwise master cannot proceed the reconfiguration.
   */
  public void completeAllOngoingSync() {
    LOG.log(Level.INFO, "Complete all ongoing syncs for {0}", ongoingSyncs.values());
    ongoingSyncs.forEach((executorId, opId) -> completeSync(opId, executorId));
  }

  /**
   * Syncs ownership cache by waiting for the unassociation of {@code executorId}.
   * @param opId an operation id
   * @param executorId an executor id
   */
  public void syncUnassociation(final long opId, final String executorId) {
    synchronized (executorIdToNumBlocks) {
      final AtomicInteger numBlocksCounter = executorIdToNumBlocks.get(executorId);
      if (numBlocksCounter != null && numBlocksCounter.get() > 0) {
        LOG.log(Level.INFO, "Sync started. opId: {0}, tableId: {1}, deletedExecutorId: {2}",
            new Object[]{opId, tableId, executorId});
        ongoingSyncs.put(executorId, opId);
        return;
      }
    }

    completeSync(opId, executorId);
  }

  /**
   * Blocks access to a block until {@link #allowAccessToBlock} is called.
   * @param blockId id of the block
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
}
