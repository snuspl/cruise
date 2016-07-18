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
import edu.snu.cay.services.em.driver.api.ElasticMemory;
import edu.snu.cay.services.em.driver.impl.BlockManager;
import edu.snu.cay.services.em.msg.api.ElasticMemoryMsgSender;
import edu.snu.cay.services.evalmanager.api.EvaluatorManager;
import edu.snu.cay.services.ps.common.parameters.NumServers;
import edu.snu.cay.services.ps.driver.impl.EMRoutingTable;
import edu.snu.cay.services.ps.worker.impl.WorkerMsgSender;
import org.apache.reef.io.network.naming.NameClient;
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

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

/**
 * Tests for DynamicServerResolver.
 * It checks whether DynamicServerResolver is initialized and updated correctly, and resolves the correct server.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({WorkerMsgSender.class, NameClient.class})
public class DynamicServerResolverTest {
  private static final String SERVER_ID_PREFIX = "SERVER-";
  private static final int NUM_SERVERS = 5;
  private static final int NUM_TOTAL_BLOCKS = 128;

  private DynamicServerResolver serverResolver;

  private ElasticMemory serverEM;

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
    driverInjector.bindVolatileInstance(ElasticMemoryMsgSender.class, mock(ElasticMemoryMsgSender.class));
    serverEM = driverInjector.getInstance(ElasticMemory.class);
    final BlockManager blockManager = driverInjector.getInstance(BlockManager.class);

    storeIdToEndpointIdBiMap = HashBiMap.create();

    // Register all servers to block manager
    for (int serverIdx = 0; serverIdx < NUM_SERVERS; serverIdx++) {
      final String endpointId = SERVER_ID_PREFIX + serverIdx;
      final int storeId = blockManager.registerEvaluator(endpointId, NUM_SERVERS);
      storeIdToEndpointIdBiMap.put(storeId, endpointId);
    }

    // ElasticMemory gets storeIdToBlockIds information from blockManager
    final EMRoutingTable initRoutingTable
        = new EMRoutingTable(serverEM.getStoreIdToBlockIds(), storeIdToEndpointIdBiMap);

    // 2. worker-side setup
    final Configuration workerConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(NumTotalBlocks.class, Integer.toString(NUM_TOTAL_BLOCKS))
        .build();

    final Injector workerInjector = Tang.Factory.getTang().newInjector(workerConf);
    workerInjector.bindVolatileInstance(NameClient.class, mock(NameClient.class));

    msgSender = mock(WorkerMsgSender.class);
    workerInjector.bindVolatileInstance(WorkerMsgSender.class, msgSender);

    serverResolver = workerInjector.getInstance(DynamicServerResolver.class);

    // reset init latch
    initLatch = new CountDownLatch(1);

    doAnswer(new Answer() {
      @Override
      public Object answer(final InvocationOnMock invocation) throws Throwable {

        serverResolver.initRoutingTable(initRoutingTable);
        initLatch.countDown();
        return null;
      }
    }).when(msgSender).sendWorkerRegisterMsg();
  }

  /**
   * Tests resolver after explicitly initializing the routing table.
   */
  @Test
  public void testResolveAfterExplicitInit() throws InterruptedException {
    final int numKey = 1000;

    serverResolver.requestRoutingTable();

    initLatch.await();

    final Map<Integer, Set<Integer>> storeIdToBlockIds = serverEM.getStoreIdToBlockIds();

    for (int hash = 0; hash < numKey; hash++) {
      final int blockId = getBlockId(hash);

      final String serverEndpointId = serverResolver.resolveServer(hash);
      final int storeId = storeIdToEndpointIdBiMap.inverse().get(serverEndpointId);
      final Set<Integer> blockSet = storeIdToBlockIds.get(storeId);
      assertTrue(blockSet.contains(blockId));
    }

    verify(msgSender, times(1)).sendWorkerRegisterMsg();
  }

  /**
   * Tests resolver without explicit initialization of the routing table.
   * The routing table will be initialized automatically by {@link DynamicServerResolver#resolveServer(int)}.
   */
  @Test
  public void testResolveWithoutExplicitInit() {
    final int numKey = 1000;

    final Map<Integer, Set<Integer>> storeIdToBlockIds = serverEM.getStoreIdToBlockIds();

    for (int hash = 0; hash < numKey; hash++) {
      final int blockId = hash % NUM_TOTAL_BLOCKS;

      final String serverEndpointId = serverResolver.resolveServer(hash);
      final int storeId = storeIdToEndpointIdBiMap.inverse().get(serverEndpointId);
      final Set<Integer> blockSet = storeIdToBlockIds.get(storeId);
      assertTrue(blockSet.contains(blockId));
    }

    verify(msgSender, times(1)).sendWorkerRegisterMsg();
  }

  /**
   * Tests whether resolvers are correctly updated by
   * {@link DynamicServerResolver#updateRoutingTable(EMRoutingTableUpdate)}.
   */
  @Test
  public void testUpdateResolver() {
    final int numKey = 1000;

    serverResolver.requestRoutingTable();

    // Update the routing table by migrating a block that contains the key
    for (int hash = 0; hash < numKey; hash++) {
      final int blockId = getBlockId(hash);

      final String oldServerEndpointId = serverResolver.resolveServer(hash);

      final int oldStoreId = storeIdToEndpointIdBiMap.inverse().get(oldServerEndpointId);
      final int newStoreId = (oldStoreId + 1) % NUM_SERVERS;
      final String newEvalId = storeIdToEndpointIdBiMap.get(newStoreId);

      final List<Integer> blockIds = new ArrayList<>(1);
      blockIds.add(blockId);

      final EMRoutingTableUpdate routingTableUpdate
          = new EMRoutingTableUpdateImpl(oldStoreId, newStoreId, newEvalId, blockIds);

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
    private final List<Integer> blockIds;

    EMRoutingTableUpdateImpl(final int oldOwnerId,
                             final int newOwnerId,
                             final String newEvalId,
                             final List<Integer> blockIds) {
      this.oldOwnerId = oldOwnerId;
      this.newOwnerId = newOwnerId;
      this.newEvalId = newEvalId;
      this.blockIds = blockIds;
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
    public List<Integer> getBlockIds() {
      return blockIds;
    }
  }
}
