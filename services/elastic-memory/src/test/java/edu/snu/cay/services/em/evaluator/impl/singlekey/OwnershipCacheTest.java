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
package edu.snu.cay.services.em.evaluator.impl.singlekey;

import edu.snu.cay.services.em.avro.*;
import edu.snu.cay.services.em.common.parameters.MemoryStoreId;
import edu.snu.cay.services.em.common.parameters.NumInitialEvals;
import edu.snu.cay.services.em.common.parameters.NumTotalBlocks;
import edu.snu.cay.services.em.driver.impl.BlockManager;
import edu.snu.cay.services.em.driver.impl.EMMsgHandler;
import edu.snu.cay.services.em.evaluator.impl.OwnershipCache;
import edu.snu.cay.services.em.msg.api.EMMsgSender;
import edu.snu.cay.utils.ThreadUtils;
import org.apache.reef.io.network.impl.NSMessage;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.htrace.TraceInfo;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

/**
 * Tests to check whether {@link OwnershipCache} is initialized correctly, and manages ownership correctly.
 */
public class OwnershipCacheTest {
  // TODO #509: EM assumes that the eval prefix has "-" at the end
  private static final String EVAL_ID_PREFIX = "EVAL-";

  private Injector driverInjector;
  private LinkedList<OwnershipCache> ownershipCaches;

  @Before
  public void setup(final int numInitialEvals,
                    final int numTotalBlocks) {
    final Configuration driverConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(NumTotalBlocks.class, Integer.toString(numTotalBlocks))
        .bindNamedParameter(NumInitialEvals.class, Integer.toString(numInitialEvals))
        .build();

    driverInjector = Tang.Factory.getTang().newInjector(driverConf);

    final EMMsgSender driverMsgSender = mock(EMMsgSender.class);
    driverInjector.bindVolatileInstance(EMMsgSender.class, driverMsgSender);

    ownershipCaches = new LinkedList<>();

    // driverMsgHander.onNext will invoke driverMsgSender.sendOwnershipCacheInitMsg with the routine table
    doAnswer(invocation -> {
      final List<Integer> blockLocations = invocation.getArgumentAt(1, List.class);
      for (final OwnershipCache cache : ownershipCaches) {
        cache.initOwnershipInfo(blockLocations);
      }
      return null;
    }).when(driverMsgSender).sendOwnershipCacheInitMsg(anyString(), anyList(), any(TraceInfo.class));
  }

  private OwnershipCache newOwnershipCache(final int numInitialEvals,
                                           final int numTotalBlocks,
                                           final int memoryStoreId) throws InjectionException {
    // 1. setup eval-side components that is common for all stores
    final Configuration evalConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(NumInitialEvals.class, Integer.toString(numInitialEvals))
        .bindNamedParameter(NumTotalBlocks.class, Integer.toString(numTotalBlocks))
        .bindNamedParameter(MemoryStoreId.class, Integer.toString(memoryStoreId))
        .build();
    final Injector evalInjector = Tang.Factory.getTang().newInjector(evalConf);

    final EMMsgSender evalMsgSender = mock(EMMsgSender.class);
    evalInjector.bindVolatileInstance(EMMsgSender.class, evalMsgSender);
    final OwnershipCache ownershipCache = evalInjector.getInstance(OwnershipCache.class);

    // 2.Setup eval-side msg sender and driver-side msg sender/handler and block manager.
    // By mocking msg sender and handler in both side, we can simulate more realistic system behaviors.
    final BlockManager blockManager = driverInjector.getInstance(BlockManager.class);

    // Register all eval to block manager, now this OwnershipCache can obtain the complete ownership info
    for (int evalIdx = 0; evalIdx < numInitialEvals; evalIdx++) {
      final String endpointId = EVAL_ID_PREFIX + evalIdx;

      // we assume that BlockManager.registerEvaluator() assigns store ids in the increasing order,
      // so the index of evaluator endpoint is equal to the store id
      blockManager.registerEvaluator(endpointId, numInitialEvals);
    }

    final EMMsgHandler driverMsgHandler = driverInjector.getInstance(EMMsgHandler.class);

    final IdentifierFactory identifierFactory = driverInjector.getInstance(StringIdentifierFactory.class);

    // dummy ids to generate a network message
    final String driverId = "driver";
    final String evalId = "eval";
    final Identifier evalIdentifier = identifierFactory.getNewInstance(evalId);
    final Identifier driverIdentifier = identifierFactory.getNewInstance(driverId);

    doAnswer(invocation -> {
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
    }).when(evalMsgSender).sendOwnershipCacheInitReqMsg(any(TraceInfo.class));

    return ownershipCache;
  }

  /**
   * Tests whether source and destination {@link OwnershipCache}s are
   * correctly updated by {@link OwnershipCache#updateOwnership}.
   */
  @Test
  public void testUpdatingOwnership() throws InjectionException {
    final int numTotalBlocks = 1024;
    final int numInitialMemoryStores = 4;

    final int srcStoreId = 0;
    final OwnershipCache srcOwnershipCache = newOwnershipCache(numInitialMemoryStores, numTotalBlocks, srcStoreId);

    final List<Integer> srcInitialBlocks = srcOwnershipCache.getInitialLocalBlockIds();
    List<Integer> srcCurrentBlocks = srcOwnershipCache.getCurrentLocalBlockIds();

    assertEquals("OwnershipCache is initialized incorrectly", srcInitialBlocks.size(), srcCurrentBlocks.size());
    assertTrue("OwnershipCache is initialized incorrectly", srcInitialBlocks.containsAll(srcCurrentBlocks));

    final int destStoreId = 1;
    final OwnershipCache destOwnershipCache = newOwnershipCache(numInitialMemoryStores, numTotalBlocks, destStoreId);

    // move the half of blocks between two evaluators by updating OwnershipCaches
    final int numBlocksToMove = srcInitialBlocks.size() / 2;
    final List<Integer> movedBlocks = new ArrayList<>(numBlocksToMove);
    for (int i = 0; i < numBlocksToMove; i++) {
      final int movingBlockId = srcInitialBlocks.get(i);
      srcOwnershipCache.updateOwnership(movingBlockId, srcStoreId, destStoreId);
      destOwnershipCache.updateOwnership(movingBlockId, srcStoreId, destStoreId);
      movedBlocks.add(movingBlockId);
    }

    // check that the OwnershipCaches are correctly updated as expected
    srcCurrentBlocks = srcOwnershipCache.getCurrentLocalBlockIds();
    final List<Integer> destCurrentBlocks = destOwnershipCache.getCurrentLocalBlockIds();
    final List<Integer> destInitialBlocks = destOwnershipCache.getInitialLocalBlockIds();

    assertEquals("The number of current blocks in source OwnershipCache has not been updated correctly",
        srcInitialBlocks.size() - numBlocksToMove, srcCurrentBlocks.size());
    assertEquals("The number of current blocks in destination OwnershipCache has not been updated correctly",
        destInitialBlocks.size() + numBlocksToMove, destCurrentBlocks.size());
    assertTrue("Current blocks in source OwnershipCache have not been updated correctly",
        srcInitialBlocks.containsAll(srcCurrentBlocks));
    assertTrue("Current blocks in destination OwnershipCache have not been updated correctly",
        destCurrentBlocks.containsAll(destInitialBlocks));

    for (final int blockId : movedBlocks) {
      assertFalse("This block should have been moved out from source OwnershipCache",
          srcCurrentBlocks.contains(blockId));
      assertTrue("This block should have been moved into destination OwnershipCache",
          destCurrentBlocks.contains(blockId));
    }
  }


  /**
   * Tests whether {@link OwnershipCache}s are correctly locked by {@link OwnershipCache#resolveStoreWithLock}
   * to prevent themselves from being updated by {@link OwnershipCache#updateOwnership}.
   */
  @Test
  public void testLockingOwnershipCacheFromUpdate() throws InjectionException, InterruptedException {
    final int numTotalBlocks = 1024;
    final int numInitialMemoryStores = 4;
    final int numBlocksToMove = 2; // should be smaller than (numTotalBlocks/numInitialMemoryStores)

    final int storeId = 0;
    final OwnershipCache ownershipCache = newOwnershipCache(numInitialMemoryStores, numTotalBlocks, storeId);

    final List<Integer> initialBlocks = ownershipCache.getInitialLocalBlockIds();
    final List<Integer> blocksToMove = initialBlocks.subList(0, numBlocksToMove);

    // Resolving a single block locks the whole ownership cache
    final Lock ownershipCacheLock = ownershipCache.resolveStoreWithLock(blocksToMove.get(0)).getValue();

    final int destStoreId = 1;

    final CountDownLatch updateLatch = new CountDownLatch(numBlocksToMove);
    final ExecutorService ownershipUpdateExecutor = Executors.newFixedThreadPool(numBlocksToMove);
    for (final int blockId : blocksToMove) {
      ownershipUpdateExecutor.submit(() -> {
        ownershipCache.updateOwnership(blockId, storeId, destStoreId);
        updateLatch.countDown();
      });
    }

    assertFalse("Thread should not be finished before unlock OwnershipCache",
        updateLatch.await(2000, TimeUnit.MILLISECONDS));

    final List<Integer> curBlocksBeforeUnlock = ownershipCache.getCurrentLocalBlockIds();
    assertEquals("Routing table should not be updated", initialBlocks, curBlocksBeforeUnlock);

    // unlock OwnershipCache to let threads update it
    ownershipCacheLock.unlock();

    assertTrue("Thread should be finished after unlock", updateLatch.await(2000, TimeUnit.MILLISECONDS));

    blocksToMove.forEach(curBlocksBeforeUnlock::remove);
    final List<Integer> curBlocksAfterUnlock = ownershipCache.getCurrentLocalBlockIds();
    assertEquals("Routing table should be updated", curBlocksBeforeUnlock, curBlocksAfterUnlock);

    ownershipUpdateExecutor.shutdown();
  }

  /**
   * Checks whether MemoryStores share the same view on the ownership status initially.
   */
  @Test
  public void testMultipleOwnershipCaches() throws InjectionException {
    final int numTotalBlocks = 1024;
    final int numMemoryStores = 4;

    final OwnershipCache[] ownershipCacheArray = new OwnershipCache[numMemoryStores];
    for (int storeId = 0; storeId < numMemoryStores; storeId++) {
      ownershipCacheArray[storeId] = newOwnershipCache(numMemoryStores, numTotalBlocks, storeId);
    }

    for (int blockId = 0; blockId < numTotalBlocks; blockId++) {

      // This is the memory store id that is answered at the first time.
      // It is for checking all OwnershipCaches give the same answer.
      // -1 means that memory store id for the block has not been found yet
      int firstAnswer = -1;

      // check all OwnershipCaches give same answer
      for (int storeId = 0; storeId < numMemoryStores; storeId++) {
        final int blockOwner = ownershipCacheArray[storeId].resolveStore(blockId);

        if (firstAnswer == -1) {
          firstAnswer = blockOwner; // it's set by the first OwnershipCache's answer
        } else {
          assertEquals("Routers should give the same memory store id for the same block", firstAnswer, blockOwner);
        }
      }
    }
  }

  /**
   * Tests OwnershipCaches after initializing them.
   * The test checks whether initialization is performed, and
   * runs multiple threads using resolver to check whether the correct result is given.
   */
  @Test
  public void testRouterInAddedEvalAfterInit() throws InjectionException, InterruptedException {
    final int numTotalBlocks = 1024;
    final int numInitialMemoryStores = 4;
    final int numThreads = 8;

    final CountDownLatch threadLatch = new CountDownLatch(numThreads);

    final int storeId0 = 0;
    final int storeId1 = 1;

    final OwnershipCache ownershipCache0 = newOwnershipCache(numInitialMemoryStores, numTotalBlocks, storeId0);
    final OwnershipCache ownershipCache1 = newOwnershipCache(numInitialMemoryStores, numTotalBlocks, storeId1);

    ownershipCache0.triggerInitialization();
    ownershipCache1.triggerInitialization();

    // confirm that the OwnershipCaches are initialized now
    assertTrue(ownershipCache0.isInitialized());
    assertTrue(ownershipCache1.isInitialized());

    final Runnable[] threads = new Runnable[numThreads];

    // their views on the global ownership should be equal
    for (int idx = 0; idx < numThreads; idx++) {
      threads[idx] = () -> {
        for (int blockId = 0; blockId < numTotalBlocks; blockId++) {
          final Integer storeIdFromCache0 = ownershipCache0.resolveStore(blockId);
          final Integer storeIdFromCache1 = ownershipCache1.resolveStore(blockId);

          assertEquals(storeIdFromCache0, storeIdFromCache1);
        }
      };

      ThreadUtils.runConcurrently(threads);
      assertTrue(threadLatch.await(30, TimeUnit.SECONDS));
    }
  }

  /**
   * Tests OwnershipCaches without explicit initialization.
   * The test runs multiple threads using resolver to check
   * whether the threads are blocked until the initialization and the correct result is given.
   */
  @Test
  public void testOwnershipCacheBeforeInit() throws InjectionException, InterruptedException {
    final int numTotalBlocks = 1024;
    final int numInitialMemoryStores = 4;
    final int numThreads = 8;

    final CountDownLatch threadLatch = new CountDownLatch(numThreads);

    final int storeId0 = 0;
    final int storeId1 = 1;

    final OwnershipCache ownershipCache0 = newOwnershipCache(numInitialMemoryStores, numTotalBlocks, storeId0);
    final OwnershipCache ownershipCache1 = newOwnershipCache(numInitialMemoryStores, numTotalBlocks, storeId1);

    // While multiple threads access OwnershipCache, they will wait until the initialization is done.
    final Runnable[] threads = new Runnable[numThreads];

    // their views on the global ownership should be equal
    for (int idx = 0; idx < numThreads; idx++) {
      threads[idx] = () -> {
        for (int blockId = 0; blockId < numTotalBlocks; blockId++) {
          final Integer storeIdFromCache0 = ownershipCache0.resolveStore(blockId);
          final Integer storeIdFromCache1 = ownershipCache1.resolveStore(blockId);

          assertEquals(storeIdFromCache0, storeIdFromCache1);
        }
        threadLatch.countDown();
      };
    }

    ThreadUtils.runConcurrently(threads);

    // make threads wait for initialization
    Thread.sleep(5000);
    assertEquals("Threads should not progress before initialization", numThreads, threadLatch.getCount());

    // confirm that the OwnershipCaches are not initialized yet
    assertFalse(ownershipCache0.isInitialized());
    assertFalse(ownershipCache1.isInitialized());

    ownershipCache0.triggerInitialization();
    ownershipCache1.triggerInitialization();

    // confirm that the OwnershipCaches are initialized now
    assertTrue(ownershipCache0.isInitialized());
    assertTrue(ownershipCache1.isInitialized());

    assertTrue(threadLatch.await(30, TimeUnit.SECONDS));
  }
}
