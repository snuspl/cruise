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
package edu.snu.cay.services.ps.driver.impl;

import edu.snu.cay.services.em.avro.*;
import edu.snu.cay.services.em.common.parameters.NumTotalBlocks;
import edu.snu.cay.services.em.driver.api.EMMaster;
import edu.snu.cay.services.em.driver.impl.BlockManager;
import edu.snu.cay.services.em.driver.impl.EMMsgHandler;
import edu.snu.cay.services.em.msg.api.EMMsgSender;
import edu.snu.cay.services.ps.avro.AvroPSMsg;
import edu.snu.cay.services.ps.avro.Type;
import edu.snu.cay.services.ps.common.parameters.NumServers;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.io.network.group.impl.utils.ResettingCountDownLatch;
import org.apache.reef.io.network.impl.NSMessage;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.htrace.SpanReceiver;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link EMRoutingTableManager}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(PSMessageSender.class)
public final class EMRoutingTableManagerTest {
  private static final int NUM_TOTAL_BLOCKS = 1024;
  private static final int NUM_SERVERS = 5;
  private static final int NUM_WORKERS = 5;

  // TODO #509: Remove EM's assumption on the format of context Id
  private static final String SERVER_ID_PREFIX = "SERVER-";
  private static final String WORKER_ID_PREFIX = "WORKER-";

  private EMRoutingTableManager emRoutingTableManager;

  private EMMaster serverEMMaster;
  private BlockManager blockManager; // a sub component of serverEMMaster

  private PSMessageSender mockPSSender;

  private EMMsgSender mockEMMsgSender;
  private EMMsgHandler emMsgHandler;

  @Before
  public void setup() throws InjectionException {
    final Configuration conf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(NumTotalBlocks.class, Integer.toString(NUM_TOTAL_BLOCKS))
        .bindNamedParameter(NumServers.class, Integer.toString(NUM_SERVERS))
        .build();

    final Injector injector = Tang.Factory.getTang().newInjector(conf);
    injector.bindVolatileInstance(EvaluatorRequestor.class, mock(EvaluatorRequestor.class));
    injector.bindVolatileInstance(SpanReceiver.class, mock(SpanReceiver.class));

    mockPSSender = mock(PSMessageSender.class);
    injector.bindVolatileInstance(PSMessageSender.class, mockPSSender);

    mockEMMsgSender = mock(EMMsgSender.class);
    injector.bindVolatileInstance(EMMsgSender.class, mockEMMsgSender);

    emMsgHandler = injector.getInstance(EMMsgHandler.class);
    serverEMMaster = injector.getInstance(EMMaster.class);
    blockManager = injector.getInstance(BlockManager.class);
    emRoutingTableManager = injector.getInstance(EMRoutingTableManager.class);
  }

  /**
   * Tests whether RoutingTableManager broadcasts the initial routing table to workers,
   * after all servers have been registered.
   */
  @Test(timeout = 20000)
  public void testInitializingWorkers() throws InterruptedException {
    final int numEarlyWorkers = NUM_WORKERS / 2;
    final ResettingCountDownLatch countDownLatch = new ResettingCountDownLatch(numEarlyWorkers);
    final Set<String> workerIds = new HashSet<>();

    // mock PS msg sender to monitor on sending a worker replay msg
    doAnswer(invocation -> {
      final String workerId = invocation.getArgumentAt(0, String.class);
      final AvroPSMsg msg = invocation.getArgumentAt(1, AvroPSMsg.class);

      assertTrue(workerIds.remove(workerId));
      assertEquals(Type.WorkerRegisterReplyMsg, msg.getType());
      countDownLatch.countDown();
      return null;
    }).when(mockPSSender).send(anyString(), anyObject());

    // 1. Register early workers
    for (int workerIdx = 0; workerIdx < numEarlyWorkers; workerIdx++) {
      final String workerId = WORKER_ID_PREFIX + workerIdx;
      workerIds.add(workerId);
      emRoutingTableManager.registerWorker(workerId);
    }

    // workers cannot receive the routing table until all the servers have been registered
    verify(mockPSSender, times(0)).send(anyString(), anyObject());

    // 2. Register all servers
    for (int serverIdx = 0; serverIdx < NUM_SERVERS; serverIdx++) {
      final String endpointId = SERVER_ID_PREFIX + serverIdx;
      final int storeId = blockManager.registerEvaluator(endpointId, NUM_SERVERS);

      // initial routing tables will be sent out after all the servers are registered finally
      verify(mockPSSender, times(0)).send(anyString(), anyObject());
      emRoutingTableManager.registerServer(storeId, endpointId);
    }

    // early workers will receive the initial routing tables
    countDownLatch.awaitAndReset(NUM_WORKERS - numEarlyWorkers);
    verify(mockPSSender, times(numEarlyWorkers)).send(anyString(), anyObject());

    // 3. Register remaining workers
    for (int workerIdx = numEarlyWorkers; workerIdx < NUM_WORKERS; workerIdx++) {
      final String workerId = WORKER_ID_PREFIX + workerIdx;
      workerIds.add(workerId);
      emRoutingTableManager.registerWorker(workerId);
    }

    // workers that register lately also should receive the routing tables
    countDownLatch.await();
    verify(mockPSSender, times(NUM_WORKERS)).send(anyString(), anyObject());
    assertTrue("WorkerIds should be empty", workerIds.isEmpty());
  }

  /**
   * Tests whether EMRoutingTableManager broadcasts new updates in the routing table to workers.
   */
  @Test(timeout = 20000)
  public void testBroadcastingUpdate() {
    // register servers
    for (int serverIdx = 0; serverIdx < NUM_SERVERS; serverIdx++) {
      final String endpointId = SERVER_ID_PREFIX + serverIdx;
      blockManager.registerEvaluator(endpointId, NUM_SERVERS);
    }

    // register workers
    for (int workerIdx = 0; workerIdx < NUM_WORKERS; workerIdx++) {
      final String workerId = WORKER_ID_PREFIX + workerIdx;
      emRoutingTableManager.registerWorker(workerId);
    }

    // mock EM msg sender to simulate migration
    doAnswer(invocation -> {
      final String senderId = invocation.getArgumentAt(0, String.class);
      final String receiverId = invocation.getArgumentAt(1, String.class);
      final List<Integer> blocks = invocation.getArgumentAt(2, List.class);
      final String opId = invocation.getArgumentAt(3, String.class);

      // OwnershipAckMsg and BlockMovedMsg will finish the migration
      for (final int blockId : blocks) {
        final OwnershipAckMsg ownershipAckMsg = OwnershipAckMsg.newBuilder()
            .setOldOwnerId(getStoreId(senderId))
            .setNewOwnerId(getStoreId(receiverId))
            .setBlockId(blockId)
            .build();

        MigrationMsg migrationMsg = MigrationMsg.newBuilder()
            .setType(MigrationMsgType.OwnershipAckMsg)
            .setOperationId(opId)
            .setOwnershipAckMsg(ownershipAckMsg)
            .build();

        EMMsg emMsg = EMMsg.newBuilder()
            .setType(EMMsgType.MigrationMsg)
            .setMigrationMsg(migrationMsg)
            .build();

        emMsgHandler.onNext(new NSMessage<>(null, null, emMsg));

        final BlockMovedMsg blockMovedMsg = BlockMovedMsg.newBuilder()
            .setBlockId(blockId)
            .build();

        migrationMsg = MigrationMsg.newBuilder()
            .setType(MigrationMsgType.BlockMovedMsg)
            .setOperationId(opId)
            .setBlockMovedMsg(blockMovedMsg)
            .build();

        emMsg = EMMsg.newBuilder()
            .setType(EMMsgType.MigrationMsg)
            .setMigrationMsg(migrationMsg)
            .build();

        emMsgHandler.onNext(new NSMessage<>(null, null, emMsg));
      }
      return null;
    }).when(mockEMMsgSender)
        .sendMoveInitMsg(anyString(), anyString(), anyListOf(Integer.class), anyString(), anyObject());

    final int numBlocksToMove = 5;
    final int numFirstMoves = 2;
    final int numSecondMoves = 5;

    // The below test assumes that requested number of blocks will be always moved for simplicity.
    // So we should assure that the number of blocks in a src server is enough for two times of moves.
    final int numBlocksInOneServer = NUM_TOTAL_BLOCKS / NUM_SERVERS;
    assertTrue(numBlocksInOneServer > numBlocksToMove * (numFirstMoves + numSecondMoves));

    final int numFirstUpdates = numFirstMoves * numBlocksToMove * NUM_WORKERS;
    final int numSecondUpdates = numSecondMoves * numBlocksToMove * NUM_WORKERS;
    final String srcServerId = SERVER_ID_PREFIX + 0;
    final String destServerId = SERVER_ID_PREFIX + 1;

    final ResettingCountDownLatch countDownLatch = new ResettingCountDownLatch(numFirstUpdates);

    // mock PS msg sender to monitor on sending a routing update msg
    doAnswer(invocation -> {
      final AvroPSMsg msg = invocation.getArgumentAt(1, AvroPSMsg.class);

      assertEquals(Type.RoutingTableUpdateMsg, msg.getType());
      countDownLatch.countDown();
      return null;
    }).when(mockPSSender).send(anyString(), anyObject());

    for (int i = 0; i < numFirstMoves; i++) {
      serverEMMaster.move(numBlocksToMove, srcServerId, destServerId, null);
    }

    countDownLatch.awaitAndReset(numSecondUpdates);
    verify(mockPSSender, times(numFirstUpdates)).send(anyString(), anyObject());

    for (int i = 0; i < numSecondMoves; i++) {
      serverEMMaster.move(numBlocksToMove, srcServerId, destServerId, null);
    }

    countDownLatch.await();
    verify(mockPSSender, times(numFirstUpdates + numSecondUpdates)).send(anyString(), anyObject());
  }

  /**
   * Converts evaluator id to store id.
   * TODO #509: remove assumption on the format of context Id
   */
  private int getStoreId(final String evalId) {
    // MemoryStoreId is the suffix of context id (Please refer to PartitionManager.registerEvaluator()
    // and EMConfProvider.getServiceConfigurationWithoutNameResolver()).
    return Integer.valueOf(evalId.split("-")[1]);
  }
}
