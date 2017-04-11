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
package edu.snu.cay.services.et.driver.impl;

import edu.snu.cay.common.dataloader.HdfsSplitInfo;
import edu.snu.cay.services.et.configuration.TableConfiguration;
import edu.snu.cay.services.et.driver.api.MessageSender;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EventHandler;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test that the MigrationManager handles the states and sends messages correctly.
 */
public final class MigrationManagerTest {
  /**
   * The number of initial executors should be larger than 3 for simulating all roles: sender, receiver, and the other
   * so as to test the broadcast of the result of migration in {@link #testBroadcastUpdate()}.
   */
  private static final int NUM_INIT_EXECUTORS = 3;

  /**
   * Time interval for waiting broadcast and update notification of migration result.
   * It is required for detecting whether migration manager performs broadcast/update more than it should do,
   * by waiting enough amount of time. We determine this with the number of intervals taken by normal operations.
   */
  private static final int WAIT_INTERVAL_MS = 100;

  // this test deals only one table
  private static final String TABLE_ID = "table";
  private BlockManager blockManager;

  private MigrationManager migrationManager;
  private MockedMsgSender messageSender;

  private static final String EXECUTOR_ID_PREFIX = "Executor-";
  private static final int NUM_REQUESTS_PER_EXECUTOR = 10;
  private static final int NUM_THREAD = 4;

  @Before
  public void setUp() throws InjectionException {
    final Configuration conf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(MessageSender.class, MockedMsgSender.class)
        .build();

    final Injector injector = Tang.Factory.getTang().newInjector(conf);

    blockManager = injector.getInstance(BlockManager.class);
    messageSender = (MockedMsgSender) injector.getInstance(MessageSender.class);
    migrationManager = injector.getInstance(MigrationManager.class);

    // initialize BlockManager and MigrationManager with executors
    final Set<String> initExecutors = new HashSet<>();
    for (int i = 0; i < NUM_INIT_EXECUTORS; i++) {
      final String executorId = EXECUTOR_ID_PREFIX + i;
      initExecutors.add(executorId);
      migrationManager.registerSubscription(TABLE_ID, executorId);
    }
    blockManager.init(initExecutors);
  }

  /**
   * Test concurrent migration by multiple threads.
   * Test move data blocks from the first evaluators to the second evaluator and vice versa.
   */
  @Test
  public void testMigration() throws InterruptedException {
    final ExecutorService executor = Executors.newFixedThreadPool(NUM_THREAD);

    // movedLatch is countdown in FinishedCallback.onNext()
    final CountDownLatch movedLatch = new CountDownLatch(2 * NUM_REQUESTS_PER_EXECUTOR);
    final FinishedCallback finishedCallback0 = new FinishedCallback(movedLatch);
    final FinishedCallback finishedCallback1 = new FinishedCallback(movedLatch);

    final int numInitBlocksIn0 = blockManager.getNumBlocks(EXECUTOR_ID_PREFIX + 0);
    final int numInitBlocksIn1 = blockManager.getNumBlocks(EXECUTOR_ID_PREFIX + 1);

    // the actual number of moved blocks can be different, so need to check with finishedCallback.getNumMovedBlocks
    final int numBlocksToMoveFromExecutor0 = numInitBlocksIn0 / NUM_REQUESTS_PER_EXECUTOR;
    final int numBlocksToMoveFromExecutor1 = numInitBlocksIn1 / NUM_REQUESTS_PER_EXECUTOR;

    // Start migration of blocks between two executors, by requesting multiple times
    for (int i = 0; i < NUM_REQUESTS_PER_EXECUTOR; i++) {
      executor.execute(new MigrationThread(TABLE_ID, EXECUTOR_ID_PREFIX + 0, EXECUTOR_ID_PREFIX + 1,
          numBlocksToMoveFromExecutor0, finishedCallback0, blockManager, migrationManager));
      executor.execute(new MigrationThread(TABLE_ID, EXECUTOR_ID_PREFIX + 1, EXECUTOR_ID_PREFIX + 0,
          numBlocksToMoveFromExecutor1, finishedCallback1, blockManager, migrationManager));
    }

    // wait until all moves are finished
    assertTrue("Move does not finish within time", movedLatch.await(20000, TimeUnit.MILLISECONDS));

    // check that the final number of blocks in both executors are as expected
    final int numMovedBlocksFrom0To1 = finishedCallback0.getNumMovedBlocks();
    final int numMovedBlocksFrom1To0 = finishedCallback1.getNumMovedBlocks();

    final int numBlocksIn0 = numInitBlocksIn0 + numMovedBlocksFrom1To0 - numMovedBlocksFrom0To1;
    final int numBlocksIn1 = numInitBlocksIn1 + numMovedBlocksFrom0To1 - numMovedBlocksFrom1To0;

    assertEquals("The number of blocks is different from expectation",
        numBlocksIn0, blockManager.getNumBlocks(EXECUTOR_ID_PREFIX + 0));
    assertEquals("The number of blocks is different from expectation",
        numBlocksIn1, blockManager.getNumBlocks(EXECUTOR_ID_PREFIX + 1));
  }

  @Test(timeout = 20000)
  public void testBroadcastUpdate() throws InterruptedException {
    final ExecutorService executor = Executors.newFixedThreadPool(NUM_THREAD);
    int numOtherExecutors = NUM_INIT_EXECUTORS - 2; // exclude sender and receiver.

    // 1. First moves
    final int numMoves = 5;

    // movedLatch is countdown in FinishedCallback.onNext()
    final CountDownLatch movedLatch0 = new CountDownLatch(numMoves);
    final FinishedCallback finishedCallback0 = new FinishedCallback(movedLatch0);

    runRandomMove(executor, numMoves, NUM_INIT_EXECUTORS, finishedCallback0);
    assertTrue("Move does not finish within time", movedLatch0.await(10000, TimeUnit.MILLISECONDS));

    int waitLoop = 0;
    final int numUpdatesInFirstMoves = finishedCallback0.getNumMovedBlocks() * numOtherExecutors;
    while (messageSender.getBroadcastCount() < numUpdatesInFirstMoves) {
      waitLoop++;
      Thread.sleep(WAIT_INTERVAL_MS);
    }
    // confirm the number of broadcast stops
    Thread.sleep(WAIT_INTERVAL_MS * (waitLoop + 1)); // +1 to make it sure
    assertEquals("There are more updates above expectation",
        numUpdatesInFirstMoves, messageSender.getBroadcastCount());

    // 2. Second moves with a new subscriber
    final int newExecutorIdx = NUM_INIT_EXECUTORS;
    migrationManager.registerSubscription(TABLE_ID, EXECUTOR_ID_PREFIX + newExecutorIdx);
    numOtherExecutors++;

    // movedLatch is countdown in FinishedCallback.onNext()
    final CountDownLatch movedLatch1 = new CountDownLatch(numMoves);
    final FinishedCallback finishedCallback1 = new FinishedCallback(movedLatch1);

    runRandomMove(executor, numMoves, NUM_INIT_EXECUTORS, finishedCallback1);
    assertTrue("Move does not finish within time", movedLatch1.await(10000, TimeUnit.MILLISECONDS));

    waitLoop = 0;
    final int numUpdatesInSecondMoves = finishedCallback1.getNumMovedBlocks() * numOtherExecutors;
    while (messageSender.getBroadcastCount() < numUpdatesInFirstMoves + numUpdatesInSecondMoves) {
      waitLoop++;
      Thread.sleep(WAIT_INTERVAL_MS);
    }

    // confirm the number of broadcast stops
    Thread.sleep(WAIT_INTERVAL_MS * (waitLoop + 1)); // +1 to make it sure
    assertEquals("There are more updates above expectation",
        numUpdatesInFirstMoves + numUpdatesInSecondMoves, messageSender.getBroadcastCount());
  }

  /**
   * Callback called when the migration is finished.
   */
  private final class FinishedCallback implements EventHandler<MigrationResult> {

    /**
     * A latch for counting finished moves, including both succeeded and failed ones.
     */
    private final CountDownLatch movedLatch;

    /**
     * A counter for the total number of moved blocks.
     */
    private final AtomicInteger movedBlockCounter = new AtomicInteger(0);

    FinishedCallback(final CountDownLatch movedLatch) {
      this.movedLatch = movedLatch;
    }

    /**
     * @return the number of moved blocks
     */
    int getNumMovedBlocks() {
      return movedBlockCounter.get();
    }

    @Override
    public void onNext(final MigrationResult migrationResult) {
      final List<Integer> movedBlocks = migrationResult.getMigratedBlocks();
      movedBlockCounter.addAndGet(movedBlocks.size());
      movedLatch.countDown();
    }
  }

  /**
   * Run moves between two evaluators randomly selected among given associated executors.
   * @param numMoves the number of moves
   * @param numAssociatedExecutors the active number of executors
   * @param callback the callback called when each move is finished
   */
  private void runRandomMove(final ExecutorService executor,
                             final int numMoves, final int numAssociatedExecutors,
                             final EventHandler<MigrationResult> callback) {
    final Random random = new Random();

    for (int i = 0; i < numMoves; i++) {
      final int srcIdx = random.nextInt(numAssociatedExecutors);
      final int destIdx = (srcIdx + 1) % numAssociatedExecutors;

      final int numBlocksToMove = blockManager.getNumBlocks(EXECUTOR_ID_PREFIX + srcIdx) / numMoves;
      executor.execute(new MigrationThread(TABLE_ID, EXECUTOR_ID_PREFIX + srcIdx, EXECUTOR_ID_PREFIX + destIdx,
          numBlocksToMove, callback, blockManager, migrationManager));
    }
  }

  /**
   * A thread that performs migration with given parameters.
   */
  private static final class MigrationThread implements Runnable {
    private final String tableId;
    private final String srcId;
    private final String destId;
    private final int numBlocksToMove;
    private final EventHandler<MigrationResult> finishedCallback;
    private final BlockManager blockManager;
    private final MigrationManager migrationManager;

    MigrationThread(final String tableId, final String srcId, final String destId, final int numBlocksToMove,
                    final EventHandler<MigrationResult> finishedCallback,
                    final BlockManager blockManager,
                    final MigrationManager migrationManager) {
      this.tableId = tableId;
      this.srcId = srcId;
      this.destId = destId;
      this.numBlocksToMove = numBlocksToMove;
      this.finishedCallback = finishedCallback;
      this.blockManager = blockManager;
      this.migrationManager = migrationManager;
    }

    @Override
    public void run() {
      final List<Integer> blockIdsToMove = blockManager.chooseBlocksToMove(srcId, numBlocksToMove);
      migrationManager.startMigration(blockManager, tableId, srcId, destId, blockIdsToMove)
          .addListener(finishedCallback);
    }
  }

  /**
   * Mocked message sender simulates migration without actual network communications.
   */
  private static final class MockedMsgSender implements MessageSender {

    /**
     * A counter for the number of broadcasts.
     */
    private final AtomicInteger updateBroadcastCounter = new AtomicInteger(0);

    private final InjectionFuture<MigrationManager> migrationManager;

    /**
     * @return the number of broadcasts
     */
    int getBroadcastCount() throws InterruptedException {
      return updateBroadcastCounter.get();
    }

    @Inject
    private MockedMsgSender(final InjectionFuture<MigrationManager> migrationManager) {
      this.migrationManager = migrationManager;
    }

    @Override
    public void sendTableInitMsg(final String executorId, final TableConfiguration tableConf,
                                 final List<String> blockOwnerList, @Nullable final HdfsSplitInfo fileSplit) {

    }

    @Override
    public void sendTableDropMsg(final String executorId, final String tableId) {

    }

    @Override
    public void sendOwnershipUpdateMsg(final String executorId,
                                       final String tableId, final int blockId,
                                       final String oldOwnerId, final String newOwnerId) {
      // called when move is finished, so as to broadcast the change of the routing table
      updateBroadcastCounter.getAndIncrement();
    }

    @Override
    public void sendMoveInitMsg(final long opId, final String tableId, final List<Integer> blockIds,
                                final String senderId, final String receiverId) {
      for (final int blockId : blockIds) {
        migrationManager.get().ownershipMoved(opId, blockId);
        migrationManager.get().markBlockAsMoved(opId, blockId);
      }
    }
  }
}
