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

import edu.snu.cay.services.em.avro.*;
import edu.snu.cay.services.em.common.parameters.AddedEval;
import edu.snu.cay.services.em.common.parameters.MemoryStoreId;
import edu.snu.cay.services.em.common.parameters.NumInitialEvals;
import edu.snu.cay.services.em.common.parameters.NumTotalBlocks;
import edu.snu.cay.services.em.driver.impl.BlockManager;
import edu.snu.cay.services.em.driver.impl.EMMsgHandler;
import edu.snu.cay.services.em.msg.api.EMMsgSender;
import edu.snu.cay.utils.ThreadUtils;
import org.apache.reef.io.network.impl.NSMessage;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.htrace.TraceInfo;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

/**
 * Tests to check whether OwnershipCache is initialized correctly, and give correct answer to route operations.
 */
public class OwnershipCacheTest {
  // TODO #509: EM assumes that the eval prefix has "-" at the end
  private static final String EVAL_ID_PREFIX = "EVAL-";

  private CountDownLatch initLatch;

  /**
   * Creates an instance of OwnershipCache based on the input parameters.
   * @param numInitialEvals the number of initial evaluators
   * @param numTotalBlocks the number of total blocks
   * @param memoryStoreId the local memory store id
   * @param addedEval a boolean representing whether or not an evaluator is added by EM.add()
   * @return an instance of OwnershipCache
   * @throws InjectionException
   */
  private OwnershipCache newOwnershipCache(final int numInitialEvals,
                                           final int numTotalBlocks,
                                           final int memoryStoreId,
                                           final boolean addedEval) throws InjectionException {
    // 1. setup eval-side components that is common for all ownership caches
    final Configuration evalConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(NumInitialEvals.class, Integer.toString(numInitialEvals))
        .bindNamedParameter(NumTotalBlocks.class, Integer.toString(numTotalBlocks))
        .bindNamedParameter(MemoryStoreId.class, Integer.toString(memoryStoreId))
        .bindNamedParameter(AddedEval.class, Boolean.toString(addedEval))
        .build();
    final Injector evalInjector = Tang.Factory.getTang().newInjector(evalConf);

    final EMMsgSender evalMsgSender = mock(EMMsgSender.class);
    evalInjector.bindVolatileInstance(EMMsgSender.class, evalMsgSender);
    final OwnershipCache ownershipCache = evalInjector.getInstance(OwnershipCache.class);

    // 2. If it is a ownershipCache for a newly added evaluator,
    // setup eval-side msg sender and driver-side msg sender/handler and block manager.
    // By mocking msg sender and handler in both side, we can simulate more realistic system behaviors.
    if (addedEval) {
      final Configuration driverConf = Tang.Factory.getTang().newConfigurationBuilder()
          .bindNamedParameter(NumTotalBlocks.class, Integer.toString(numTotalBlocks))
          .bindNamedParameter(AddedEval.class, Boolean.toString(addedEval))
          .bindNamedParameter(MemoryStoreId.class, Integer.toString(memoryStoreId))
          .build();

      final Injector driverInjector = Tang.Factory.getTang().newInjector(driverConf);
      final BlockManager blockManager = driverInjector.getInstance(BlockManager.class);

      // Register all eval to block manager, now this ownershipCache can obtain the complete ownership info
      for (int evalIdx = 0; evalIdx < numInitialEvals; evalIdx++) {
        final String endpointId = EVAL_ID_PREFIX + evalIdx;

        // we assume that BlockManager.registerEvaluator() assigns store ids in the increasing order,
        // so the index of evaluator endpoint is equal to the store id
        blockManager.registerEvaluator(endpointId, numInitialEvals);
      }

      final EMMsgSender driverMsgSender = mock(EMMsgSender.class);
      driverInjector.bindVolatileInstance(EMMsgSender.class, driverMsgSender);

      final EMMsgHandler driverMsgHandler = driverInjector.getInstance(EMMsgHandler.class);

      final IdentifierFactory identifierFactory = driverInjector.getInstance(StringIdentifierFactory.class);

      // dummy ids to generate a network message
      final String driverId = "driver";
      final String evalId = "eval";
      final Identifier evalIdentifier = identifierFactory.getNewInstance(evalId);
      final Identifier driverIdentifier = identifierFactory.getNewInstance(driverId);

      doAnswer(new Answer() {
        @Override
        public Object answer(final InvocationOnMock invocation) throws Throwable {
          Thread.sleep(1000); // delay for fetching the ownership info from driver

          final OwnershipCacheInitReqMsg ownershipCacheInitReqMsg = OwnershipCacheInitReqMsg.newBuilder()
              .setEvalId(evalId)
              .build();

          final OwnershipCacheMsg ownershipCacheMsg = OwnershipCacheMsg.newBuilder()
              .setType(OwnershipCacheMsgType.OwnershipCacheInitReqMsg)
              .setOwnershipCacheInitReqMsg(ownershipCacheInitReqMsg)
              .build();

          final EMMsg msg = EMMsg.newBuilder()
              .setType(EMMsgType.OwnershipCacheMsg)
              .setOwnershipCacheMsg(ownershipCacheMsg)
              .build();

          driverMsgHandler.onNext(new NSMessage<>(evalIdentifier, driverIdentifier, msg));
          return null;
        }
      }).when(evalMsgSender).sendOwnershipCacheInitReqMsg(any(TraceInfo.class));

      // reset initLatch
      initLatch = new CountDownLatch(1);

      // driverMsgHander.onNext will invoke driverMsgSender.sendOwnershipCacheInitMsg with the ownership info
      doAnswer(new Answer() {
        @Override
        public Object answer(final InvocationOnMock invocation) throws Throwable {
          final List<Integer> blockLocations = invocation.getArgumentAt(1, List.class);
          ownershipCache.initWithDriver(blockLocations);
          initLatch.countDown();
          return null;
        }
      }).when(driverMsgSender).sendOwnershipCacheInitMsg(anyString(), anyList(), any(TraceInfo.class));
    }

    return ownershipCache;
  }

  /**
   * Checks whether blocks assigned to each MemoryStore have its unique owner (MemoryStore),
   * and the local blocks acquired from getInitialLocalBlockIds() belong to the local MemoryStore.
   */
  @Test
  public void testLocalBlocks() throws InjectionException {
    final int numTotalBlocks = 1024;
    final int numMemoryStores = 4;

    final Set<Integer> totalBlocks = new HashSet<>(numTotalBlocks);

    for (int localStoreId = 0; localStoreId < numMemoryStores; localStoreId++) {
      final OwnershipCache ownershipCache =
          newOwnershipCache(numMemoryStores, numTotalBlocks, localStoreId, false);

      final List<Integer> localBlockIds = ownershipCache.getInitialLocalBlockIds();

      for (final int blockId : localBlockIds) {
        // OwnershipCache.resolveEval(blockId) returns empty when the MemoryStore owns the block locally
        assertFalse("Ownership cache fails to classify local blocks",
            ownershipCache.resolveEval(blockId).isPresent());
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
    final int numMemoryStores = 4;

    final OwnershipCache[] ownershipCaches = new OwnershipCache[numMemoryStores];
    for (int storeId = 0; storeId < numMemoryStores; storeId++) {
      ownershipCaches[storeId] = newOwnershipCache(numMemoryStores, numTotalBlocks, storeId, false);
    }

    for (int blockId = 0; blockId < numTotalBlocks; blockId++) {

      // This is the memory store id that is answered at the first time.
      // It is for checking all ownershipCaches give the same answer.
      // -1 means that memory store id for the block has not been found yet
      int firstAnswer = -1;

      boolean localStoreFound = false;

      // check all ownershipCaches give same answer
      for (int storeId = 0; storeId < numMemoryStores; storeId++) {
        final Optional<String> evalId = ownershipCaches[storeId].resolveEval(blockId);

        final int targetStoreId;
        // OwnershipCache.resolveEval(blockId) returns empty when the MemoryStore owns the block locally
        if (!evalId.isPresent()) {
          assertFalse("Block should belong to only one store", localStoreFound);
          localStoreFound = true;

          targetStoreId = storeId;
        } else {
          // TODO #509: remove the assumption on the format of context id
          targetStoreId = Integer.valueOf(evalId.get().split("-")[1]);
        }

        if (firstAnswer == -1) {
          firstAnswer = targetStoreId; // it's set by the first OwnershipCache's answer
        } else {
          assertEquals("Ownership caches should give the same memory store id for the same block",
              firstAnswer, targetStoreId);
        }
      }
    }
  }

  /**
   * Tests whether ownership caches are correctly updated by {@link OwnershipCache#updateOwnership(int, int, int)}.
   */
  @Test
  public void testUpdatingOwnership() throws InjectionException {
    final int numTotalBlocks = 1024;
    final int numInitialMemoryStores = 4;

    final int srcStoreId = 0;
    final OwnershipCache srcOwnershipCache =
        newOwnershipCache(numInitialMemoryStores, numTotalBlocks, srcStoreId, false);

    final List<Integer> srcInitialBlocks = srcOwnershipCache.getInitialLocalBlockIds();
    List<Integer> srcCurrentBlocks = srcOwnershipCache.getCurrentLocalBlockIds();

    assertEquals("Ownership cache is initialized incorrectly", srcInitialBlocks.size(), srcCurrentBlocks.size());
    assertTrue("Owenership cache is initialized incorrectly", srcInitialBlocks.containsAll(srcCurrentBlocks));

    final int destStoreId = 1;
    final OwnershipCache dstOwnershipCache =
        newOwnershipCache(numInitialMemoryStores, numTotalBlocks, destStoreId, false);

    // move the half of blocks between two evaluators by updating OwnershipCaches
    final int numBlocksToMove = srcInitialBlocks.size() / 2;
    final List<Integer> movedBlocks = new ArrayList<>(numBlocksToMove);
    for (int i = 0; i < numBlocksToMove; i++) {
      final int movingBlockId = srcInitialBlocks.get(i);
      srcOwnershipCache.updateOwnership(movingBlockId, srcStoreId, destStoreId);
      dstOwnershipCache.updateOwnership(movingBlockId, srcStoreId, destStoreId);
      movedBlocks.add(movingBlockId);
    }

    // check that the OwnsershipCache is correctly updated as expected
    srcCurrentBlocks = srcOwnershipCache.getCurrentLocalBlockIds();
    final List<Integer> destCurrentBlocks = dstOwnershipCache.getCurrentLocalBlockIds();
    final List<Integer> destInitialBlocks = dstOwnershipCache.getInitialLocalBlockIds();

    assertEquals("The number of current blocks in source ownership cache has not been updated correctly",
        srcInitialBlocks.size() - numBlocksToMove, srcCurrentBlocks.size());
    assertEquals("The number of current blocks in destination ownership cache has not been updated correctly",
        destInitialBlocks.size() + numBlocksToMove, destCurrentBlocks.size());
    assertTrue("Current blocks in source ownership cache have not been updated correctly",
        srcInitialBlocks.containsAll(srcCurrentBlocks));
    assertTrue("Current blocks in destination ownership cache have not been updated correctly",
        destCurrentBlocks.containsAll(destInitialBlocks));

    for (final int blockId : movedBlocks) {
      assertFalse("This block should have been moved out from source ownership cache",
          srcCurrentBlocks.contains(blockId));
      assertTrue("This block should have been moved into destination ownership cache",
          destCurrentBlocks.contains(blockId));
    }
  }

  /**
   * Tests whether OwnershipCaches are correctly locked by {@link OwnershipCache#resolveEvalWithLock}
   * to prevent themselves from being updated by {@link OwnershipCache#updateOwnership}.
   */
  @Test
  public void testLockingOwnershipCacheFromUpdate() throws InjectionException, InterruptedException {
    final int numTotalBlocks = 1024;
    final int numInitialMemoryStores = 4;
    final int numBlocksToMove = 2; // should be smaller than (numTotalBlocks/numInitialMemoryStores)

    final int storeId = 0;
    final OwnershipCache ownershipCache = newOwnershipCache(numInitialMemoryStores, numTotalBlocks, storeId, false);

    final List<Integer> initialBlocks = ownershipCache.getInitialLocalBlockIds();
    final List<Integer> blocksToMove = initialBlocks.subList(0, numBlocksToMove);

    // hold read-locks on blocks to move
    final List<Lock> locks = new ArrayList<>(numBlocksToMove);

    for (final int blockId : blocksToMove) {
      final Lock blockLock = ownershipCache.resolveEvalWithLock(blockId).getValue();
      locks.add(blockLock);
    }

    final int destStoreId = 1;

    final CountDownLatch updateLatch = new CountDownLatch(numBlocksToMove);
    final ExecutorService ownershipUpdateExecutor = Executors.newFixedThreadPool(numBlocksToMove);
    for (final int blockId : blocksToMove) {
      ownershipUpdateExecutor.submit(() -> {
        ownershipCache.updateOwnership(blockId, storeId, destStoreId);
        updateLatch.countDown();
      });
    }

    assertFalse("Thread should not be finished before unlock ownershipCache",
        updateLatch.await(2000, TimeUnit.MILLISECONDS));

    final List<Integer> curBlocksBeforeUnlock = ownershipCache.getCurrentLocalBlockIds();
    assertEquals("Ownership cache should not be updated", initialBlocks, curBlocksBeforeUnlock);

    // unlock ownershipCache to let threads update the ownershipCache
    locks.forEach(Lock::unlock);

    assertTrue("Thread should be finished after unlock", updateLatch.await(2000, TimeUnit.MILLISECONDS));

    blocksToMove.forEach(curBlocksBeforeUnlock::remove);
    final List<Integer> curBlocksAfterUnlock = ownershipCache.getCurrentLocalBlockIds();
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
    final int numInitialMemoryStores = 4;
    final int blockId0 = 0;
    final int blockId1 = 1;

    final int storeId = 0;
    final OwnershipCache ownershipCache = newOwnershipCache(numInitialMemoryStores, numTotalBlocks, storeId, false);

    ownershipCache.updateOwnership(blockId1, 1, storeId);
    final ExecutorService blockResolvingExecutor = Executors.newFixedThreadPool(2);

    final CountDownLatch resolvedLatch1 = new CountDownLatch(1);
    blockResolvingExecutor.submit(() -> {
      ownershipCache.resolveEvalWithLock(blockId1);
      resolvedLatch1.countDown();
    });

    final CountDownLatch resolvedLatch0 = new CountDownLatch(1);
    blockResolvingExecutor.submit(() -> {
      ownershipCache.resolveEvalWithLock(blockId0);
      resolvedLatch0.countDown();
    });

    assertFalse("Thread should not be finished before unlock", resolvedLatch1.await(2000, TimeUnit.MILLISECONDS));
    assertTrue("Other blocks should not be accessed", resolvedLatch0.await(2000, TimeUnit.MILLISECONDS));

    // unlock ownershipCache to let threads update the ownershipCache
    ownershipCache.allowAccessToBlock(blockId1);

    assertTrue("Thread should be finished after unlock", resolvedLatch1.await(2000, TimeUnit.MILLISECONDS));

    blockResolvingExecutor.shutdown();
  }

  /**
   * Tests OwnershipCache after initialization.
   * Stores in added evaluators are initialized by communicating with the driver,
   * and stores in initial evaluators are initialized by itself without any communication.
   * The test checks whether initialization is performed, and
   * runs multiple threads using resolver to check whether the correct result is given.
   */
  @Test
  public void testOwnershipCacheInAddedEvalAfterInit() throws InjectionException, InterruptedException {
    final int numTotalBlocks = 1024;
    final int numInitialMemoryStores = 4;
    final int numThreads = 8;

    final CountDownLatch threadLatch = new CountDownLatch(numThreads);

    // because we compare the whole set of ownership caches in other tests,
    // this test focus on comparing only two ownership caches initialized in different ways
    final int initStoreId = 0;
    final int addedStoreId = 4; // It should be larger than the largest index of initial stores

    // ownership cache in initStore will be initialized statically
    final OwnershipCache ownershipCacheInInitStore
        = newOwnershipCache(numInitialMemoryStores, numTotalBlocks, initStoreId, false);
    // ownership cache in addedStore will be initialized dynamically
    final OwnershipCache ownershipCacheInAddedStore
        = newOwnershipCache(numInitialMemoryStores, numTotalBlocks, addedStoreId, true);

    // we assume that store ids are assigned in the increasing order,
    // so the index of evaluator endpoint is equal to the store id
    final String endpointIdForInitEval = EVAL_ID_PREFIX + initStoreId;
    final String endpointIdForAddedEval = EVAL_ID_PREFIX + addedStoreId;

    ownershipCacheInInitStore.setEndpointIdPrefix(endpointIdForInitEval);
    ownershipCacheInInitStore.triggerInitialization();
    ownershipCacheInAddedStore.setEndpointIdPrefix(endpointIdForAddedEval);
    ownershipCacheInAddedStore.triggerInitialization();

    // confirm that the ownership cache is initialized
    assertTrue(initLatch.await(10, TimeUnit.SECONDS));

    // While multiple threads use ownership cache,
    // the initialization never be triggered because it's already initialized.
    final Runnable[] threads = new Runnable[numThreads];

    // though init ownership cache is statically initialized and added ownership cache is dynamically initialized,
    // because there were no changes in the ownership info their views should be equal
    for (int idx = 0; idx < numThreads; idx++) {
      threads[idx] = new Runnable() {
        @Override
        public void run() {
          for (int blockId = 0; blockId < numTotalBlocks; blockId++) {
            final Optional<String> evalIdFromInitOwnershipCache = ownershipCacheInInitStore.resolveEval(blockId);
            final Optional<String> evalIdFromAddedOwnershipCache = ownershipCacheInAddedStore.resolveEval(blockId);

            if (!evalIdFromInitOwnershipCache.isPresent()) { // ownershipCacheInInitStore is local
              assertEquals(endpointIdForInitEval, evalIdFromAddedOwnershipCache.get());
            } else if (!evalIdFromAddedOwnershipCache.isPresent()) { // ownershipCacheInAddedStore is local
              assertEquals(endpointIdForAddedEval, evalIdFromInitOwnershipCache.get());
            } else {
              assertEquals(evalIdFromInitOwnershipCache.get(), evalIdFromAddedOwnershipCache.get());
            }
          }
          threadLatch.countDown();
        }
      };
    }

    ThreadUtils.runConcurrently(threads);
    assertTrue(threadLatch.await(30, TimeUnit.SECONDS));
  }

  /**
   * Tests OwnershipCache without explicit initialization.
   * Stores in added evaluators are initialized by communicating with the driver,
   * and stores in initial evaluators are initialized by itself without any communication.
   * The test runs multiple threads using resolver to check
   * whether the threads are blocked until the initialization and the correct result is given.
   */
  @Test
  public void testOwnershipCacheInAddedEvalBeforeInit() throws InjectionException, InterruptedException {
    final int numTotalBlocks = 1024;
    final int numInitialMemoryStores = 4;
    final int numThreads = 8;

    final CountDownLatch threadLatch = new CountDownLatch(numThreads);

    // because we compare the whole set of ownership caches in other tests,
    // this test focus on comparing only two ownership caches initialized in different ways
    final int initStoreId = 0;
    final int addedStoreId = 4; // It should be larger than the largest index of initial stores

    // ownership cache in initStore will be initialized statically
    final OwnershipCache ownershipCacheInInitStore
        = newOwnershipCache(numInitialMemoryStores, numTotalBlocks, initStoreId, false);
    // ownership cache in addedStore will be initialized dynamically
    final OwnershipCache ownershipCacheInAddedStore
        = newOwnershipCache(numInitialMemoryStores, numTotalBlocks, addedStoreId, true);

    // we assume that store ids are assigned in the increasing order,
    // so the index of evaluator endpoint is equal to the store id
    final String endpointIdForInitEval = EVAL_ID_PREFIX + initStoreId;
    final String endpointIdForAddedEval = EVAL_ID_PREFIX + addedStoreId;

    // While multiple threads use ownership cache, they will wait until the initialization is done.
    final Runnable[] threads = new Runnable[numThreads];

    // though init ownership cache is statically initialized and added ownership cache is dynamically initialized,
    // because there were no changes in the ownership info their views should be equal
    for (int idx = 0; idx < numThreads; idx++) {
      threads[idx] = new Runnable() {
        @Override
        public void run() {
          for (int blockId = 0; blockId < numTotalBlocks; blockId++) {
            final Optional<String> evalIdFromInitOwnershipCache = ownershipCacheInInitStore.resolveEval(blockId);
            final Optional<String> evalIdFromAddedOwnershipCache = ownershipCacheInAddedStore.resolveEval(blockId);

            if (!evalIdFromInitOwnershipCache.isPresent()) { // ownershipCacheInInitStore is local
              assertEquals(endpointIdForInitEval, evalIdFromAddedOwnershipCache.get());
            } else if (!evalIdFromAddedOwnershipCache.isPresent()) { // ownershipCacheInAddedStore is local
              assertEquals(endpointIdForAddedEval, evalIdFromInitOwnershipCache.get());
            } else {
              assertEquals(evalIdFromInitOwnershipCache.get(), evalIdFromAddedOwnershipCache.get());
            }
          }
          threadLatch.countDown();
        }
      };
    }

    ThreadUtils.runConcurrently(threads);

    // make threads wait for initialization
    Thread.sleep(5000);
    assertEquals("Threads should not progress before initialization", numThreads, threadLatch.getCount());

    // confirm that the ownership cache is not initialized yet
    assertEquals(1, initLatch.getCount());

    ownershipCacheInInitStore.setEndpointIdPrefix(endpointIdForInitEval);
    ownershipCacheInInitStore.triggerInitialization();
    ownershipCacheInAddedStore.setEndpointIdPrefix(endpointIdForAddedEval);
    ownershipCacheInAddedStore.triggerInitialization();

    // confirm that the ownership cache is initialized now
    assertTrue(initLatch.await(10, TimeUnit.SECONDS));

    assertTrue(threadLatch.await(30, TimeUnit.SECONDS));
  }
}
