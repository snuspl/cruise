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
package edu.snu.cay.services.ps.common.resolver;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import edu.snu.cay.services.em.common.parameters.NumTotalBlocks;
import edu.snu.cay.services.em.driver.api.EMRoutingTableUpdate;
import edu.snu.cay.services.em.driver.api.EMMaster;
import edu.snu.cay.services.em.driver.impl.BlockManager;
import edu.snu.cay.services.em.msg.api.EMMsgSender;
import edu.snu.cay.services.evalmanager.api.EvaluatorManager;
import edu.snu.cay.services.ps.common.parameters.NumServers;
import edu.snu.cay.services.ps.driver.impl.EMRoutingTable;
import edu.snu.cay.services.ps.worker.impl.WorkerMsgSender;
import edu.snu.cay.utils.ThreadUtils;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.htrace.SpanReceiver;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

/**
 * Tests for DynamicServerResolver.
 * It checks whether DynamicServerResolver is initialized and updated correctly, and resolves the correct server.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(WorkerMsgSender.class)
public class DynamicServerResolverTest {
  private static final String SERVER_ID_PREFIX = "SERVER-";
  private static final int NUM_SERVERS = 5;
  private static final int NUM_TOTAL_BLOCKS = 128;

  private DynamicServerResolver serverResolver;

  private EMMaster serverEM;

  private WorkerMsgSender msgSender;

  private BiMap<Integer, String> storeIdToEndpointIdBiMap;

  private CountDownLatch initLatch;

  @Before
  public void setup() throws InjectionException {
    // 1. driver-side setup
    final Configuration serverConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(NumServers.class, Integer.toString(NUM_SERVERS))
        .bindNamedParameter(NumTotalBlocks.class, Integer.toString(NUM_TOTAL_BLOCKS))
        .build();

    final Injector driverInjector = Tang.Factory.getTang().newInjector(serverConf);
    driverInjector.bindVolatileInstance(EvaluatorManager.class, mock(EvaluatorManager.class));
    driverInjector.bindVolatileInstance(SpanReceiver.class, mock(SpanReceiver.class));
    driverInjector.bindVolatileInstance(EMMsgSender.class, mock(EMMsgSender.class));
    serverEM = driverInjector.getInstance(EMMaster.class);
    final BlockManager blockManager = driverInjector.getInstance(BlockManager.class);

    storeIdToEndpointIdBiMap = HashBiMap.create();

    // Register all servers to block manager
    for (int serverIdx = 0; serverIdx < NUM_SERVERS; serverIdx++) {
      final String endpointId = SERVER_ID_PREFIX + serverIdx;
      final int storeId = blockManager.registerEvaluator(endpointId, NUM_SERVERS);
      storeIdToEndpointIdBiMap.put(storeId, endpointId);
    }

    // EMMaster gets storeIdToBlockIds information from blockManager
    final EMRoutingTable initRoutingTable
        = new EMRoutingTable(serverEM.getStoreIdToBlockIds(), storeIdToEndpointIdBiMap);

    // 2. worker-side setup
    final Configuration workerConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(NumTotalBlocks.class, Integer.toString(NUM_TOTAL_BLOCKS))
        .build();

    final Injector workerInjector = Tang.Factory.getTang().newInjector(workerConf);

    msgSender = mock(WorkerMsgSender.class);
    workerInjector.bindVolatileInstance(WorkerMsgSender.class, msgSender);

    serverResolver = workerInjector.getInstance(DynamicServerResolver.class);

    // reset init latch
    initLatch = new CountDownLatch(1);

    doAnswer(new Answer() {
      @Override
      public Object answer(final InvocationOnMock invocation) throws Throwable {
        Thread.sleep(1000); // delay for fetching the routing table from driver

        serverResolver.initRoutingTable(initRoutingTable);
        initLatch.countDown();
        return null;
      }
    }).when(msgSender).sendWorkerRegisterMsg();
  }

  /**
   * Tests resolver after initializing the routing table.
   * The test checks whether initialization is performed, and
   * runs multiple threads using resolver to check whether the correct result is given.
   */
  @Test
  public void testResolveAfterInit() throws InterruptedException {
    final int numKey = 1000;
    final int numThreads = 10;

    final CountDownLatch threadLatch = new CountDownLatch(numThreads);

    serverResolver.triggerInitialization();

    // confirm that the resolver is initialized
    assertTrue(initLatch.await(10, TimeUnit.SECONDS));

    final Map<Integer, Set<Integer>> storeIdToBlockIds = serverEM.getStoreIdToBlockIds();

    // While multiple threads use router, the initialization never be triggered because it's already initialized.
    final Runnable[] threads = new Runnable[numThreads];

    for (int idx = 0; idx < numThreads; idx++) {
      threads[idx] = new Runnable() {
        @Override
        public void run() {
          for (int hash = 0; hash < numKey; hash++) {
            final String serverEndpointId = serverResolver.resolveServer(hash);
            final int storeId = storeIdToEndpointIdBiMap.inverse().get(serverEndpointId);
            final Set<Integer> blockSet = storeIdToBlockIds.get(storeId);

            final int blockId = hash % NUM_TOTAL_BLOCKS;
            assertTrue(blockSet.contains(blockId));
          }
          threadLatch.countDown();
        }
      };
    }

    ThreadUtils.runConcurrently(threads);
    threadLatch.await(30, TimeUnit.SECONDS);
  }

  /**
   * Tests resolver by using it before initialization of the routing table.
   * The test runs multiple threads using resolver to check
   * whether the threads are blocked until the initialization and the correct result is given.
   */
  @Test
  public void testResolveBeforeInit() throws InterruptedException {
    final int numKey = 1000;
    final int numThreads = 10;

    final CountDownLatch threadLatch = new CountDownLatch(numThreads);

    final Map<Integer, Set<Integer>> storeIdToBlockIds = serverEM.getStoreIdToBlockIds();

    // While multiple threads use resolver, they will wait until the initialization is done.
    final Runnable[] threads = new Runnable[numThreads];

    for (int idx = 0; idx < numThreads; idx++) {
      threads[idx] = new Runnable() {
        @Override
        public void run() {
          for (int hash = 0; hash < numKey; hash++) {
            final String serverEndpointId = serverResolver.resolveServer(hash);
            final int storeId = storeIdToEndpointIdBiMap.inverse().get(serverEndpointId);
            final Set<Integer> blockSet = storeIdToBlockIds.get(storeId);

            final int blockId = hash % NUM_TOTAL_BLOCKS;
            assertTrue(blockSet.contains(blockId));
          }
          threadLatch.countDown();
        }
      };
    }

    ThreadUtils.runConcurrently(threads);

    // make threads wait for initialization
    Thread.sleep(5000);
    assertEquals("Threads should not progress before initialization", numThreads, threadLatch.getCount());

    // confirm that the resolver is not initialized yet
    assertEquals(1, initLatch.getCount());

    serverResolver.triggerInitialization();

    // confirm that the resolver is initialized now
    assertTrue(initLatch.await(10, TimeUnit.SECONDS));

    threadLatch.await(30, TimeUnit.SECONDS);
  }

  /**
   * Tests whether resolvers are correctly updated by
   * {@link DynamicServerResolver#updateRoutingTable(EMRoutingTableUpdate)}.
   */
  @Test
  public void testUpdateResolver() {
    final int numKey = 1000;

    serverResolver.triggerInitialization();

    // Update the routing table by migrating a block that contains the key
    for (int hash = 0; hash < numKey; hash++) {
      final int blockId = getBlockId(hash);

      final String oldServerEndpointId = serverResolver.resolveServer(hash);

      final int oldStoreId = storeIdToEndpointIdBiMap.inverse().get(oldServerEndpointId);
      final int newStoreId = (oldStoreId + 1) % NUM_SERVERS;
      final String newEvalId = storeIdToEndpointIdBiMap.get(newStoreId);

      final EMRoutingTableUpdate routingTableUpdate
          = new EMRoutingTableUpdateImpl(oldStoreId, newStoreId, newEvalId, blockId);

      serverResolver.updateRoutingTable(routingTableUpdate);

      final String newServerEndpointId = serverResolver.resolveServer(hash);

      // confirm that resolver answers different server ids for the same key
      assertFalse(oldServerEndpointId.equals(newServerEndpointId));

      // confirm that the update is done as we intended
      assertEquals(newEvalId, newServerEndpointId);
    }
  }

  /**
   * This method assumes that DynamicServerResolver resolves block for the key with modular function.
   * See {@link DynamicServerResolver#resolveServer(int)}.
   */
  private int getBlockId(final int keyHash) {
    return keyHash % NUM_TOTAL_BLOCKS;
  }

  /**
   * The custom implementation of {@link EMRoutingTableUpdate}.
   */
  private final class EMRoutingTableUpdateImpl implements EMRoutingTableUpdate {

    private final int oldOwnerId;
    private final int newOwnerId;
    private final String newEvalId;
    private final int blockId;

    EMRoutingTableUpdateImpl(final int oldOwnerId,
                             final int newOwnerId,
                             final String newEvalId,
                             final int blockId) {
      this.oldOwnerId = oldOwnerId;
      this.newOwnerId = newOwnerId;
      this.newEvalId = newEvalId;
      this.blockId = blockId;
    }

    @Override
    public int getOldOwnerId() {
      return oldOwnerId;
    }

    @Override
    public int getNewOwnerId() {
      return newOwnerId;
    }

    @Override
    public String getNewEvalId() {
      return newEvalId;
    }

    @Override
    public int getBlockId() {
      return blockId;
    }
  }
}
