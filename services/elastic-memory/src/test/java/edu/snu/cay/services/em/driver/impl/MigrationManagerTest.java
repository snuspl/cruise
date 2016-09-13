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
package edu.snu.cay.services.em.driver.impl;

import edu.snu.cay.services.em.avro.*;
import edu.snu.cay.services.em.driver.api.EMRoutingTableUpdate;
import edu.snu.cay.services.em.msg.api.ElasticMemoryMsgSender;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;
import org.htrace.TraceInfo;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * Test that the MigrationManager handles the states and sends messages correctly.
 */
public class MigrationManagerTest {

  /**
   * The number of intial evals should be larger than 3 for simulating all roles: sender, receiver, and the other
   * so as to test the broadcast of the result of migration in {@link #testBroadcastWithAdditionalEvals()}.
   */
  private static final int NUM_INIT_EVALS = 3;

  /**
   * Waiting interval for wating broadcast and update notification of migration result.
   * It is required for detecting whether migration manager performs broadcast/update more than it should do,
   * by waiting enough amount of time. We determine this with the number of intervals taken by normal operations.
   */
  private static final int WAIT_INTERVAL_MS = 100;

  private BlockManager blockManager;
  private MigrationManager migrationManager;
  private MockedMsgSender messageSender;

  private static final String OP_PREFIX = "op";
  private static final String EVAL_PREFIX = "eval-";
  private static final int NUM_REQUESTS_PER_EVAL = 10;
  private static final int NUM_THREAD = 4;

  @Before
  public void setUp() throws InjectionException {
    final Configuration conf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(ElasticMemoryMsgSender.class, MockedMsgSender.class)
        .build();

    final Injector injector = Tang.Factory.getTang().newInjector(conf);

    // initialize BlockManager with 3 evaluators
    blockManager = injector.getInstance(BlockManager.class);
    blockManager.registerEvaluator(EVAL_PREFIX + 0, NUM_INIT_EVALS);
    blockManager.registerEvaluator(EVAL_PREFIX + 1, NUM_INIT_EVALS);
    blockManager.registerEvaluator(EVAL_PREFIX + 2, NUM_INIT_EVALS);

    messageSender = (MockedMsgSender) injector.getInstance(ElasticMemoryMsgSender.class);
    migrationManager = injector.getInstance(MigrationManager.class);
  }

  /**
   * Test concurrent migration by multiple threads.
   * Test move data blocks from the first evaluators to the second evaluator and vice versa.
   */
  @Test
  public void testMigration() throws InterruptedException {
    final ExecutorService executor = Executors.newFixedThreadPool(NUM_THREAD);
    final AtomicInteger operationIdCounter = new AtomicInteger(0);

    // movedLatch is countdown in FinishedCallback.onNext()
    final CountDownLatch movedLatch = new CountDownLatch(2 * NUM_REQUESTS_PER_EVAL);
    final FinishedCallback finishedCallback0 = new FinishedCallback(movedLatch);
    final FinishedCallback finishedCallback1 = new FinishedCallback(movedLatch);

    final int numInitBlocksIn0 = blockManager.getNumBlocks(EVAL_PREFIX + 0);
    final int numInitBlocksIn1 = blockManager.getNumBlocks(EVAL_PREFIX + 1);

    // Start migration of blocks between Eval0 and Eval1, by requesting multiple times as many as NUM_REQUESTS_PER_EVAL.
    for (int i = 0; i < NUM_REQUESTS_PER_EVAL; i++) {
      final int numBlocksInSrcEval0 = blockManager.getNumBlocks(EVAL_PREFIX + 0);
      final int numBlocksInSrcEval1 = blockManager.getNumBlocks(EVAL_PREFIX + 1);

      final int numBlocksToMoveFromEval0 = numBlocksInSrcEval0 / NUM_REQUESTS_PER_EVAL;
      final int numBlocksToMoveFromEval1 = numBlocksInSrcEval1 / NUM_REQUESTS_PER_EVAL;

      executor.execute(new MigrationThread(operationIdCounter.getAndIncrement(),
          EVAL_PREFIX + 0, EVAL_PREFIX + 1, numBlocksToMoveFromEval0, finishedCallback0, migrationManager));
      executor.execute(new MigrationThread(operationIdCounter.getAndIncrement(),
          EVAL_PREFIX + 1, EVAL_PREFIX + 0, numBlocksToMoveFromEval1, finishedCallback1, migrationManager));
    }

    // wait until all moves are finished
    assertTrue("Move does not finish within time", movedLatch.await(20000, TimeUnit.MILLISECONDS));

    // check that the final number of blocks in both stores are as expected
    final int numMovedBlocksFrom0To1 = finishedCallback0.getNumMovedBlocks();
    final int numMovedBlocksFrom1To0 = finishedCallback1.getNumMovedBlocks();

    final int numBlocksIn0 = numInitBlocksIn0 + numMovedBlocksFrom1To0 - numMovedBlocksFrom0To1;
    final int numBlocksIn1 = numInitBlocksIn1 + numMovedBlocksFrom0To1 - numMovedBlocksFrom1To0;

    assertEquals("The number of blocks is different from expectation",
        numBlocksIn0, blockManager.getNumBlocks(EVAL_PREFIX + 0));
    assertEquals("The number of blocks is different from expectation",
        numBlocksIn1, blockManager.getNumBlocks(EVAL_PREFIX + 1));
  }

  /**
   * Test broadcasts of the result of migration are done well, when there are only initial evaluators.
   */
  @Test(timeout = 20000)
  public void testBroadcastwithInitialEvals() throws InterruptedException {
    final ExecutorService executor = Executors.newFixedThreadPool(NUM_THREAD);
    final AtomicInteger operationIdCounter = new AtomicInteger(0);

    final int numMoves = 5;

    final int numActiveEvals = NUM_INIT_EVALS;
    final int numOtherEvals = numActiveEvals - 2; // -2 to exclude src and dest evals

    // movedLatch is countdown in FinishedCallback.onNext()
    final CountDownLatch movedLatch = new CountDownLatch(numMoves);
    final FinishedCallback finishedCallback = new FinishedCallback(movedLatch);

    runRandomMove(executor, operationIdCounter, numMoves, numActiveEvals, finishedCallback);
    assertTrue("Move does not finish within time", movedLatch.await(20000, TimeUnit.MILLISECONDS));

    int loop = 0;
    final int numBroadcasts = numOtherEvals * (numMoves - finishedCallback.getNumFailedMoves());
    while (messageSender.getBroadcastCount() < numBroadcasts) {
      loop++;
      Thread.sleep(WAIT_INTERVAL_MS);
    }

    // confirm the number of broadcast stops at numBroadcasts
    Thread.sleep(WAIT_INTERVAL_MS * (loop + 1)); // +1 to make it sure
    assertEquals("There are more broadcasts above expectation", numBroadcasts, messageSender.getBroadcastCount());
  }

  /**
   * Test broadcasts of the result of migration are done well,
   * when there are additional evaluators besides initial ones.
   */
  @Test(timeout = 20000)
  public void testBroadcastWithAdditionalEvals() throws InterruptedException {
    final ExecutorService executor = Executors.newFixedThreadPool(NUM_THREAD);
    final AtomicInteger operationIdCounter = new AtomicInteger(0);

    final int numMoves = 5;

    final int numAdditionalEvals = 2;
    final int newEvalIdx = NUM_INIT_EVALS;
    for (int i = 0; i < numAdditionalEvals; i++) {
      blockManager.registerEvaluator(EVAL_PREFIX + (newEvalIdx + i), NUM_INIT_EVALS);
    }

    final int numActiveEvals = NUM_INIT_EVALS + numAdditionalEvals;
    final int numOtherEvals = numActiveEvals - 2; // -2 to exclude src and dest evals

    // movedLatch is countdown in FinishedCallback.onNext()
    final CountDownLatch movedLatch = new CountDownLatch(numMoves);
    final FinishedCallback finishedCallback = new FinishedCallback(movedLatch);

    runRandomMove(executor, operationIdCounter, numMoves, numActiveEvals, finishedCallback);
    assertTrue("Move does not finish within time", movedLatch.await(20000, TimeUnit.MILLISECONDS));

    int loop = 0;
    final int numBroadcasts = numOtherEvals * (numMoves - finishedCallback.getNumFailedMoves());
    while (messageSender.getBroadcastCount() < numBroadcasts) {
      loop++;
      Thread.sleep(WAIT_INTERVAL_MS);
    }

    // confirm the number of broadcast stops at numBroadcasts
    Thread.sleep(WAIT_INTERVAL_MS * (loop + 1)); // +1 to make it sure
    assertEquals("There are more broadcasts above expectation", numBroadcasts, messageSender.getBroadcastCount());
  }

  @Test(timeout = 20000)
  public void testNotifyingUpdate() throws InterruptedException {
    final ExecutorService executor = Executors.newFixedThreadPool(NUM_THREAD);
    final AtomicInteger operationIdCounter = new AtomicInteger(0);

    final String clientId0 = "TEST0";
    final UpdateCallback updateCallback0 = new UpdateCallback();
    migrationManager.registerRoutingTableUpdateCallback(clientId0, updateCallback0);

    final String clientId1 = "TEST1";
    final UpdateCallback updateCallback1 = new UpdateCallback();
    migrationManager.registerRoutingTableUpdateCallback(clientId1, updateCallback1);

    // 1. First moves with two listening clients
    int numMoves = 5;

    // movedLatch is countdown in FinishedCallback.onNext()
    CountDownLatch movedLatch = new CountDownLatch(numMoves);
    FinishedCallback finishedCallback = new FinishedCallback(movedLatch);

    runRandomMove(executor, operationIdCounter, numMoves, NUM_INIT_EVALS, finishedCallback);
    assertTrue("Move does not finish within time", movedLatch.await(10000, TimeUnit.MILLISECONDS));

    int loop = 0;
    final int numUpdatesInFirstMoves = finishedCallback.getNumMovedBlocks();
    while (updateCallback0.getUpdateCount() < numUpdatesInFirstMoves ||
        updateCallback1.getUpdateCount() < numUpdatesInFirstMoves) {
      loop++;
      Thread.sleep(WAIT_INTERVAL_MS);
    }

    // confirm the number of broadcast stops at numUpdatesInFirstMoves
    Thread.sleep(WAIT_INTERVAL_MS * (loop + 1)); // +1 to make it sure
    assertEquals("There are more updates above expectation",
        numUpdatesInFirstMoves, updateCallback0.getUpdateCount());
    assertEquals("There are more updates above expectation",
        numUpdatesInFirstMoves, updateCallback1.getUpdateCount());

    // 2. Second moves with only one client, after deregistering the other client
    migrationManager.deregisterRoutingTableUpdateCallback(clientId1);

    numMoves = 5;

    // movedLatch is countdown in FinishedCallback.onNext()
    movedLatch = new CountDownLatch(numMoves);
    finishedCallback = new FinishedCallback(movedLatch);

    runRandomMove(executor, operationIdCounter, numMoves, NUM_INIT_EVALS, finishedCallback);
    assertTrue("Move does not finish within time", movedLatch.await(10000, TimeUnit.MILLISECONDS));

    loop = 0;
    final int numUpdatesInSecondMoves = finishedCallback.getNumMovedBlocks();
    while (updateCallback0.getUpdateCount() < numUpdatesInFirstMoves + numUpdatesInSecondMoves) {
      loop++;
      Thread.sleep(WAIT_INTERVAL_MS);
    }

    // confirm the number of broadcast stops at numBroadcasts
    Thread.sleep(WAIT_INTERVAL_MS * (loop + 1)); // +1 to make it sure
    assertEquals("There are more updates above expectation",
        numUpdatesInFirstMoves + numUpdatesInSecondMoves, updateCallback0.getUpdateCount());
    assertEquals("There are more updates above expectation",
        numUpdatesInFirstMoves, updateCallback1.getUpdateCount()); // client1 should not receive second updates
  }

  /**
   * Callback called when the migration is finished.
   */
  private final class FinishedCallback implements EventHandler<AvroElasticMemoryMessage> {

    /**
     * A latch for counting finished moves, including both succeeded and failed ones.
     */
    private final CountDownLatch movedLatch;

    /**
     * A counter for the total number of moved blocks.
     */
    private final AtomicInteger movedBlockCounter = new AtomicInteger(0);

    /**
     * A counter for the number of failed moves.
     */
    private final AtomicInteger failedMoveCounter = new AtomicInteger(0);

    FinishedCallback(final CountDownLatch movedLatch) {
      this.movedLatch = movedLatch;
    }

    /**
     * @return the number of moved blocks
     */
    int getNumMovedBlocks() {
      return movedBlockCounter.get();
    }

    /**
     * @return the number of failed moves
     */
    int getNumFailedMoves() {
      return failedMoveCounter.get();
    }

    @Override
    public void onNext(final AvroElasticMemoryMessage msg) {
      final ResultMsg resultMsg = msg.getResultMsg();
      switch (resultMsg.getResult()) {
      case SUCCESS:
        final List<Integer> movedBlocks = resultMsg.getBlockIds();
        movedBlockCounter.addAndGet(movedBlocks.size());
        movedLatch.countDown();
        break;
      case FAILURE: // fail when a src eval has no blocks to move
        failedMoveCounter.getAndIncrement();
        movedLatch.countDown();
        break;
      default:
        throw new RuntimeException("Wrong result");
      }
    }
  }

  /**
   * Callback called when the migration is finished to notify the result of migration to clients.
   */
  private final class UpdateCallback implements EventHandler<EMRoutingTableUpdate> {

    /**
     * A counter for the number of update calls.
     */
    private AtomicInteger updateCounter = new AtomicInteger(0);

    /**
     * @return the number of called updates.
     */
    int getUpdateCount() throws InterruptedException {
      return updateCounter.get();
    }

    @Override
    public void onNext(final EMRoutingTableUpdate routingTableUpdate) {
      updateCounter.getAndIncrement();
    }
  }

  /**
   * Run moves between two evaluators randomly selected among given active evaluators.
   * @param numMoves the number of moves
   * @param numActiveEvals the active number of evaluators
   * @param callback the callback called when each move is finished
   */
  private void runRandomMove(final ExecutorService executor, final AtomicInteger opIdCounter,
                             final int numMoves, final int numActiveEvals,
                             final EventHandler<AvroElasticMemoryMessage> callback) {
    final Random random = new Random();

    for (int i = 0; i < numMoves; i++) {
      final int srcIdx = random.nextInt(numActiveEvals);
      final int destIdx = (srcIdx + 1) % numActiveEvals;

      final int numBlocksToMove = blockManager.getNumBlocks(EVAL_PREFIX + srcIdx) / numMoves;
      executor.execute(new MigrationThread(opIdCounter.getAndIncrement(),
          EVAL_PREFIX + srcIdx, EVAL_PREFIX + destIdx, numBlocksToMove, callback, migrationManager));
    }
  }

  /**
   * A thread that performs migration with given parameters.
   */
  private static final class MigrationThread implements Runnable {
    private final int opId;
    private final String srcId;
    private final String destId;
    private final int numBlocksToMove;
    private final EventHandler<AvroElasticMemoryMessage> finishedCallback;
    private final MigrationManager migrationManager;

    MigrationThread(final int opId, final String srcId, final String destId, final int numBlocksToMove,
                    final EventHandler<AvroElasticMemoryMessage> finishedCallback,
                    final MigrationManager migrationManager) {
      this.opId = opId;
      this.srcId = srcId;
      this.destId = destId;
      this.numBlocksToMove = numBlocksToMove;
      this.finishedCallback = finishedCallback;
      this.migrationManager = migrationManager;
    }

    @Override
    public void run() {
      migrationManager.startMigration(OP_PREFIX + opId, srcId, destId, numBlocksToMove, null, finishedCallback);
    }
  }

  /**
   * Mocked message sender simulates migration without actual network communications.
   */
  private static final class MockedMsgSender implements ElasticMemoryMsgSender {

    /**
     * A counter for the number of broadcasts.
     */
    private final AtomicInteger broadcastCounter = new AtomicInteger(0);

    private final BlockManager blockManager;

    private final MigrationManager migrationManager;

    /**
     * @return the number of broadcasts
     */
    int getBroadcastCount() throws InterruptedException {
      return broadcastCounter.get();
    }

    @Inject
    private MockedMsgSender(final BlockManager blockManager, final MigrationManager migrationManager) {
      this.blockManager = blockManager;
      this.migrationManager = migrationManager;
    }

    @Override
    public void sendRemoteOpReqMsg(final String origId, final String destId, final DataOpType operationType,
                                   final List<KeyRange> dataKeyRanges,
                                   final List<KeyValuePair> dataKVPairList, final String operationId,
                                   @Nullable final TraceInfo parentTraceInfo) {

    }

    @Override
    public void sendRemoteOpReqMsg(final String origId, final String destId, final DataOpType operationType,
                                   final DataKey dataKey, final DataValue dataValue,
                                   final String operationId, @Nullable final TraceInfo parentTraceInfo) {

    }

    @Override
    public void sendRemoteOpResultMsg(final String destId, final List<KeyValuePair> dataKVPairList,
                                      final List<KeyRange> failedRanges, final String operationId,
                                      @Nullable final TraceInfo parentTraceInfo) {

    }

    @Override
    public void sendRemoteOpResultMsg(final String destId, final DataValue dataValue, final boolean isSuccess,
                                      final String operationId, @Nullable final TraceInfo parentTraceInfo) {

    }

    @Override
    public void sendRoutingTableInitReqMsg(@Nullable final TraceInfo parentTraceInfo) {

    }

    @Override
    public void sendRoutingTableInitMsg(final String destId, final List<Integer> blockLocations,
                                        @Nullable final TraceInfo parentTraceInfo) {

    }

    @Override
    public void sendRoutingTableUpdateMsg(final String destId, final List<Integer> blocks,
                                          final String oldOwnerId, final String newOwnerId,
                                          @Nullable final TraceInfo parentTraceInfo) {
      // called when move is finished, so as to broadcast the change of the routing table
      broadcastCounter.getAndIncrement();
    }

    @Override
    public void sendCtrlMsg(final String destId, final String targetEvalId,
                            final List<Integer> blocks, final String operationId,
                            @Nullable final TraceInfo parentTraceInfo) {
      // invoke a handler logic of response for CtrlMsg
      for (final int blockId : blocks) {
        final int oldOwnerId = blockManager.getMemoryStoreId(destId);
        final int newOwnerId = blockManager.getMemoryStoreId(targetEvalId);
        migrationManager.updateOwner(operationId, blockId, oldOwnerId, newOwnerId, parentTraceInfo);
      }
    }

    @Override
    public void sendDataMsg(final String destId, final List<KeyValuePair> keyValuePairs,
                            final int blockId, final String operationId,
                            @Nullable final TraceInfo parentTraceInfo) {
    }

    @Override
    public void sendOwnershipMsg(final Optional<String> destId, final String operationId,
                                 final int blockId, final int oldOwnerId, final int newOwnerId,
                                 @Nullable final TraceInfo parentTraceInfo) {
      // invoke a handler logic of response for OwnershipMsg
      migrationManager.markBlockAsMoved(operationId, blockId, parentTraceInfo);
    }

    @Override
    public void sendOwnershipAckMsg(final String operationId, final int blockId,
                                    @Nullable final TraceInfo parentTraceInfo) {

    }

    @Override
    public void sendFailureMsg(final String operationId, final String reason,
                               @Nullable final TraceInfo parentTraceInfo) {

    }
  }
}
