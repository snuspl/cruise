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
import edu.snu.cay.services.et.driver.impl.BlockManager;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import static org.junit.Assert.*;

/**
 * Tests to check whether OwnershipCache is initialized correctly, and give correct answer to route operations.
 */
public class OwnershipCacheTest {
  private static final String EXECUTOR_ID_PREFIX = "executor-";

  /**
   * Creates an instance of OwnershipCache based on the input parameters.
   * @param numExecutors the number of initial evaluators
   * @param numTotalBlocks the number of total blocks
   * @return a list of OwnershipCaches
   * @throws InjectionException
   */
  private Map<String, OwnershipCache> newOwnershipCaches(final int numExecutors,
                                                         final int numTotalBlocks) throws InjectionException {
    // 1. setup driver-side components that is common for all ownership caches
    final Configuration driverConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(NumTotalBlocks.class, Integer.toString(numTotalBlocks))
        .build();

    final Injector driverInjector = Tang.Factory.getTang().newInjector(driverConf);
    final BlockManager blockManager = driverInjector.getInstance(BlockManager.class);
    final Set<String> executorIds = new HashSet<>();
    for (int executorIdx = 0; executorIdx < numExecutors; executorIdx++) {
      executorIds.add(EXECUTOR_ID_PREFIX + executorIdx);
    }
    blockManager.init(executorIds);
    final List<String> blockOwners = blockManager.getOwnershipStatus();


    // 2. setup eval-side components that is common for all ownership caches
    final Map<String, OwnershipCache> ownershipCaches = new HashMap<>();
    for (int executorIdx = 0; executorIdx < numExecutors; executorIdx++) {
      final String executorId = EXECUTOR_ID_PREFIX + executorIdx;

      final Configuration evalConf = Tang.Factory.getTang().newConfigurationBuilder()
          .bindNamedParameter(NumTotalBlocks.class, Integer.toString(numTotalBlocks))
          .bindNamedParameter(ExecutorIdentifier.class, executorId)
          .build();

      final Injector evalInjector = Tang.Factory.getTang().newInjector(evalConf);

      final OwnershipCache ownershipCache = evalInjector.getInstance(OwnershipCache.class);
      ownershipCache.init(blockOwners);
      ownershipCaches.put(executorId, ownershipCache);
    }

    return ownershipCaches;
  }

  /**
   * Checks whether blocks assigned to each MemoryStore have its unique owner (MemoryStore),
   * and the local blocks acquired from getInitialLocalBlockIds() belong to the local MemoryStore.
   */
  @Test
  public void testLocalBlocks() throws InjectionException {
    final int numTotalBlocks = 1024;
    final int numExecutors = 4;

    final Set<Integer> totalBlocks = new HashSet<>(numTotalBlocks);

    final Collection<OwnershipCache> ownershipCaches = newOwnershipCaches(numExecutors, numTotalBlocks).values();

    for (final OwnershipCache ownershipCache : ownershipCaches) {
      final List<Integer> localBlockIds = ownershipCache.getCurrentLocalBlockIds();

      for (final int blockId : localBlockIds) {
        // OwnershipCache.resolveEval(blockId) returns empty when the MemoryStore owns the block locally
        assertFalse("Ownership cache fails to classify local blocks",
            ownershipCache.resolveExecutor(blockId).isPresent());
        assertTrue("The same block is owned by multiple stores", totalBlocks.add(blockId));
      }
    }

    assertEquals("There are missing blocks", numTotalBlocks, totalBlocks.size());
  }

  /**
   * Checks whether MemoryStores share the same ownership info initially.
   */
  @Test
  public void testMultipleOwnershipCaches() throws InjectionException {
    final int numTotalBlocks = 1024;
    final int numExecutors = 4;

    final Map<String, OwnershipCache> ownershipCaches = newOwnershipCaches(numExecutors, numTotalBlocks);

    for (int blockId = 0; blockId < numTotalBlocks; blockId++) {

      // This is the memory store id that is answered at the first time.
      // It is for checking all ownershipCaches give the same answer.
      // null means that memory store id for the block has not been found yet
      String firstAnswer = null;

      boolean localStoreFound = false;

      // check all ownershipCaches give same answer
      for (final Map.Entry<String, OwnershipCache> entry : ownershipCaches.entrySet()) {
        final String localExecutorId = entry.getKey();
        final OwnershipCache ownershipCache = entry.getValue();

        final Optional<String> executorId = ownershipCache.resolveExecutor(blockId);

        final String targetExecutorId;
        // OwnershipCache.resolveEval(blockId) returns empty when the MemoryStore owns the block locally
        if (!executorId.isPresent()) {
          assertFalse("Block should belong to only one store", localStoreFound);
          localStoreFound = true;

          targetExecutorId = localExecutorId;
        } else {
          targetExecutorId = executorId.get();
        }

        if (firstAnswer == null) {
          firstAnswer = targetExecutorId; // it's set by the first OwnershipCache's answer
        } else {
          assertEquals("Ownership caches should give the same memory store id for the same block",
              firstAnswer, targetExecutorId);
        }
      }
    }
  }

  /**
   * Tests whether ownership caches are correctly updated by {@link OwnershipCache#update}.
   */
  @Test
  public void testOwnershipUpdate() throws InjectionException {
    final int numTotalBlocks = 1024;
    final int numExecutors = 4;

    final Map<String, OwnershipCache> ownershipCaches = newOwnershipCaches(numExecutors, numTotalBlocks);
    final Iterator<String> keyIter = ownershipCaches.keySet().iterator();
    final String srcExecutorId = keyIter.next();
    final String dstExecutorId = keyIter.next();
    final OwnershipCache srcOwnershipCache = ownershipCaches.get(srcExecutorId);
    final OwnershipCache dstOwnershipCache = ownershipCaches.get(dstExecutorId);

    final List<Integer> srcBeforeBlocks = srcOwnershipCache.getCurrentLocalBlockIds();
    final List<Integer> dstBeforeBlocks = dstOwnershipCache.getCurrentLocalBlockIds();

    // move the half of blocks between two evaluators by updating OwnershipCaches
    final int numBlocksToMove = srcBeforeBlocks.size() / 2;
    final List<Integer> movedBlocks = new ArrayList<>(numBlocksToMove);
    for (int i = 0; i < numBlocksToMove; i++) {
      final int movingBlockId = srcBeforeBlocks.get(i);
      srcOwnershipCache.update(movingBlockId, srcExecutorId, dstExecutorId);
      dstOwnershipCache.update(movingBlockId, srcExecutorId, dstExecutorId);
      movedBlocks.add(movingBlockId);
    }

    // check that the OwnsershipCache is correctly updated as expected
    final List<Integer> srcAfterBlocks = srcOwnershipCache.getCurrentLocalBlockIds();
    final List<Integer> dstAfterBlocks = dstOwnershipCache.getCurrentLocalBlockIds();

    assertEquals("The number of current blocks in source ownership cache has not been updated correctly",
        srcBeforeBlocks.size() - numBlocksToMove, srcAfterBlocks.size());
    assertEquals("The number of current blocks in destination ownership cache has not been updated correctly",
        dstBeforeBlocks.size() + numBlocksToMove, dstAfterBlocks.size());
    assertTrue("Current blocks in source ownership cache have not been updated correctly",
        srcBeforeBlocks.containsAll(srcAfterBlocks));
    assertTrue("Current blocks in destination ownership cache have not been updated correctly",
        dstAfterBlocks.containsAll(dstBeforeBlocks));

    for (final int blockId : movedBlocks) {
      assertFalse("This block should have been moved out from source ownership cache",
          srcAfterBlocks.contains(blockId));
      assertTrue("This block should have been moved into destination ownership cache",
          dstAfterBlocks.contains(blockId));
    }
  }

  /**
   * Tests whether OwnershipCaches are correctly locked by {@link OwnershipCache#resolveExecutorWithLock}
   * to prevent themselves from being updated by {@link OwnershipCache#update}.
   */
  @Test
  public void testLockingOwnershipCacheFromUpdate() throws InjectionException, InterruptedException {
    final int numTotalBlocks = 1024;
    final int numExecutors = 4;
    final int numBlocksToMove = 2; // should be smaller than (numTotalBlocks/numExecutors)

    final Map<String, OwnershipCache> ownershipCaches = newOwnershipCaches(numExecutors, numTotalBlocks);
    final Iterator<String> keyIter = ownershipCaches.keySet().iterator();
    final String srcExecutorId = keyIter.next();
    final String dstExecutorId = keyIter.next();
    final OwnershipCache srcOwnershipCache = ownershipCaches.get(srcExecutorId);

    final List<Integer> srcInitialBlocks = srcOwnershipCache.getCurrentLocalBlockIds();
    final List<Integer> blocksToMove = srcInitialBlocks.subList(0, numBlocksToMove);

    // hold read-locks on blocks to move
    final List<Lock> locks = new ArrayList<>(numBlocksToMove);

    for (final int blockId : blocksToMove) {
      final Lock blockLock = srcOwnershipCache.resolveExecutorWithLock(blockId).getValue();
      locks.add(blockLock);
    }

    final CountDownLatch updateLatch = new CountDownLatch(numBlocksToMove);
    final ExecutorService ownershipUpdateExecutor = Executors.newFixedThreadPool(numBlocksToMove);
    for (final int blockId : blocksToMove) {
      ownershipUpdateExecutor.submit(() -> {
        srcOwnershipCache.update(blockId, srcExecutorId, dstExecutorId);
        updateLatch.countDown();
      });
    }

    assertFalse("Thread should not be finished before unlock ownershipCache",
        updateLatch.await(2000, TimeUnit.MILLISECONDS));

    final List<Integer> curBlocksBeforeUnlock = srcOwnershipCache.getCurrentLocalBlockIds();
    assertEquals("Ownership cache should not be updated", srcInitialBlocks, curBlocksBeforeUnlock);

    // unlock ownershipCache to let threads update the ownershipCache
    locks.forEach(Lock::unlock);

    assertTrue("Thread should be finished after unlock", updateLatch.await(2000, TimeUnit.MILLISECONDS));

    blocksToMove.forEach(blockId -> assertTrue(curBlocksBeforeUnlock.remove(blockId)));
    final List<Integer> curBlocksAfterUnlock = srcOwnershipCache.getCurrentLocalBlockIds();
    assertEquals("Ownership cache should be updated", curBlocksBeforeUnlock, curBlocksAfterUnlock);

    ownershipUpdateExecutor.shutdown();
  }

  /**
   * Tests that resolving a newly incoming block cannot be done
   * before explicit allowance with {@link OwnershipCache#allowAccessToBlock}.
   */
  @Test
  public void testBlockingAccessBeforeAllow() throws InjectionException, InterruptedException {
    final int numTotalBlocks = 1024;
    final int numExecutors = 4;

    final Map<String, OwnershipCache> ownershipCaches = newOwnershipCaches(numExecutors, numTotalBlocks);
    final Iterator<String> keyIter = ownershipCaches.keySet().iterator();
    final String srcExecutorId = keyIter.next();
    final String dstExecutorId = keyIter.next();
    final OwnershipCache srcOwnershipCache = ownershipCaches.get(srcExecutorId);
    final OwnershipCache dstOwnershipCache = ownershipCaches.get(dstExecutorId);

    final List<Integer> srcBlocks = srcOwnershipCache.getCurrentLocalBlockIds();
    final int blockId0 = srcBlocks.get(0);
    final int blockId1 = srcBlocks.get(1);

    // access to block1 will be blocked until allowAccessToBlock is called
    dstOwnershipCache.update(blockId1, srcExecutorId, dstExecutorId);
    final ExecutorService blockResolvingExecutor = Executors.newFixedThreadPool(2);

    final CountDownLatch resolvedLatch1 = new CountDownLatch(1);
    blockResolvingExecutor.submit(() -> {
      dstOwnershipCache.resolveExecutorWithLock(blockId1);
      resolvedLatch1.countDown();
    });

    final CountDownLatch resolvedLatch0 = new CountDownLatch(1);
    blockResolvingExecutor.submit(() -> {
      dstOwnershipCache.resolveExecutorWithLock(blockId0);
      resolvedLatch0.countDown();
    });

    assertFalse("Thread should not be finished before unlock", resolvedLatch1.await(2000, TimeUnit.MILLISECONDS));
    assertTrue("Other blocks should not be accessed", resolvedLatch0.await(2000, TimeUnit.MILLISECONDS));

    // unlock ownershipCache to let threads update the ownershipCache
    dstOwnershipCache.allowAccessToBlock(blockId1);

    assertTrue("Thread should be finished after unlock", resolvedLatch1.await(2000, TimeUnit.MILLISECONDS));

    blockResolvingExecutor.shutdown();
  }
}
