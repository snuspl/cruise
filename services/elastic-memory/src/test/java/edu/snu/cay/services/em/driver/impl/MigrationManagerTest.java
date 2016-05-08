/*
 * Copyright (C) 2015 Seoul National University
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
import edu.snu.cay.services.em.msg.api.ElasticMemoryCallbackRouter;
import edu.snu.cay.services.em.msg.api.ElasticMemoryMsgSender;
import org.apache.commons.lang.math.LongRange;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.util.Optional;
import org.htrace.TraceInfo;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Test that the MigrationManager handles the states and sends messages correctly.
 */
public class MigrationManagerTest {
  private PartitionManager partitionManager;
  private MigrationManager migrationManager;
  private ElasticMemoryMsgSender messageSender;

  private static final String OP_ID = "op";
  private static final String EVAL0 = "eval0";
  private static final String EVAL1 = "eval1";
  private static final String DATA_TYPE = "dataType";
  private static final LongRange INITIAL_RANGE0 = new LongRange(0, 9);
  private static final LongRange INITIAL_RANGE1 = new LongRange(10, 19);
  private static final int NUM_REQUESTS = 10;
  private static final int NUM_THREAD = 4;

  @Before
  public void setUp() throws Exception {
    // Instantiate PartitionManager to register partitions manually. That way, we can test
    // whether the precondition checking works correctly, which is done before starting a migration.
    partitionManager = Tang.Factory.getTang().newInjector().getInstance(PartitionManager.class);

    // If the migration fails, throw a RuntimeException
    final ElasticMemoryCallbackRouter mockedCallbackRouter = mock(ElasticMemoryCallbackRouter.class);
    doThrow(RuntimeException.class).when(mockedCallbackRouter).onFailed(any(AvroElasticMemoryMessage.class));

    // Mock the message sender.
    messageSender = new MockedMsgSender();

    // Create a Migration Manager instance by binding the instances above.
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileInstance(PartitionManager.class, partitionManager);
    injector.bindVolatileInstance(ElasticMemoryCallbackRouter.class, mockedCallbackRouter);
    injector.bindVolatileInstance(ElasticMemoryMsgSender.class, messageSender);
    migrationManager = injector.getInstance(MigrationManager.class);
  }

  /**
   * Test the situations where startMigration will fail.
   */
  @Test
  public void testStartFailure() {
    final Set<LongRange> rangesToMove = new HashSet<>();
    rangesToMove.clear();
    // Try to move [0, 9]
    rangesToMove.add(new LongRange(0, 9));

    // Failure case1: There is no data registered in the evaluator (EVAL1).
    try {
      migrationManager.startMigration(OP_ID, EVAL1, EVAL0, DATA_TYPE, rangesToMove, null, null, null);
      fail();
    } catch (final RuntimeException e) {
      // SUCCESS
    }

    // Register the data to the Evaluators. EVAL0 has [0, 9] and EVAL1 has [10, 19].
    partitionManager.register(EVAL0, DATA_TYPE, INITIAL_RANGE0);
    partitionManager.register(EVAL1, DATA_TYPE, INITIAL_RANGE1);

    // Failure case2: When the evaluator does not have the data type (Undefined).
    try {
      migrationManager.startMigration(OP_ID, EVAL0, EVAL1, "Undefined", rangesToMove, null, null, null);
      fail();
    } catch (final RuntimeException e) {
      // SUCCESS
    }

    // Failure case3: When the request contains the data out of range [101, 110]
    rangesToMove.add(new LongRange(101, 110));
    try {
      migrationManager.startMigration(OP_ID, EVAL0, EVAL1, DATA_TYPE, rangesToMove, null, null, null);
      fail();
    } catch (final RuntimeException e) {
      // SUCCESS
    }
  }

  /**
   * Test concurrent migration by multiple threads.
   * Scenario:
   *  - EVAL0 has [0, 9] and EVAL1 has [10, 19]
   *  - 10 threads request to move the range. Each thread has an integer id.
   *  - Thread i moves the range of [2*i, 2*i+1] with operation id i.
   *  - Senders and Receivers are determined by their indices.
   *      EVAL0 -> EVAL1 (i < 5)
   *      EVAL1 -> EVAL0 (i >= 5)
   */
  @Test
  public void testMigration() {
    try {
      // Register the data to the Evaluators. EVAL0 has [0, 9] and EVAL1 has [10, 19].
      partitionManager.register(EVAL0, DATA_TYPE, INITIAL_RANGE0);
      partitionManager.register(EVAL1, DATA_TYPE, INITIAL_RANGE1);

      // Start migration and wait until all threads call waitUpdate().
      // This is only for testing in order to see multiple threads can update at the same time.
      // Originally, applyUpdates() only affects the migrations that are ready to update.
      final CountDownLatch waitUpdateLatch = new CountDownLatch(NUM_REQUESTS);
      final ExecutorService startExecutor = Executors.newFixedThreadPool(NUM_THREAD);

      // All startMigration() should be successful.
      for (int i = 0; i < NUM_REQUESTS; i++) {
        final int index = i;
        startExecutor.execute(new Runnable() {
          @Override
          public void run() {
            // thread0 moves [0, 1], thread1 moves [2, 3], ...
            final Set<LongRange> rangesToMove = new HashSet<>();
            rangesToMove.add(new LongRange(2 * index, 2 * index + 1));

            if (index < 5) {
              // thread0~4: EVAL0 -> EVAL1
              migrationManager.startMigration(Integer.toString(index), EVAL0, EVAL1, DATA_TYPE, rangesToMove,
                  null, null, null);
            } else {
              // thread5~9 EVAL1 -> EVAL0
              migrationManager.startMigration(Integer.toString(index), EVAL1, EVAL0, DATA_TYPE, rangesToMove,
                  null, null, null);
            }

            // We call those methods manually only for testing. This is done internally when receiver notifies
            // that it received the data.
            migrationManager.setMovedRanges(Integer.toString(index), rangesToMove);
            migrationManager.waitUpdate(Integer.toString(index));
            waitUpdateLatch.countDown();
          }
        });
      }
      // wait until all threads are ready to update.
      waitUpdateLatch.await();

      // The runnable code should be executed when applyUpdates() is called. So wait until that method is called.
      final CountDownLatch updateLatch = new CountDownLatch(1);
      for (int i = 0; i < NUM_REQUESTS; i++) {
        final ExecutorService updateExecutor = Executors.newFixedThreadPool(NUM_THREAD);
        final int index = i;
        updateExecutor.execute(new Runnable() {
          @Override
          public void run() {
            try {
              // Wait additional 10ms to make sure it is called after applyUpdate() is called.
              updateLatch.await();
              Thread.sleep(10);

              // Update begins when applyUpdate() is called, by sending updating message to the receiver first.
              // We check the order in the MockedMsgSender.sendUpdateMsg()

              // Update the receiver & get UpdateAck message.
              migrationManager.movePartition(Integer.toString(index), null);
              migrationManager.updateSender(Integer.toString(index), null);

              // Update the sender & get the UpdateAck message.
              migrationManager.finishMigration(Integer.toString(index));
            } catch (final InterruptedException e) {
              throw new RuntimeException(e);
            }
          }
        });
      }

      // All the threads start to apply updates.
      updateLatch.countDown();
      migrationManager.applyUpdates(null);

      // Check the ranges are exchanged successfully.
      assertEquals(1, partitionManager.getRangeSet(EVAL0, DATA_TYPE).size());
      for (final LongRange range0 : partitionManager.getRangeSet(EVAL0, DATA_TYPE)) {
        assertEquals(range0, INITIAL_RANGE1);
      }

      assertEquals(1, partitionManager.getRangeSet(EVAL0, DATA_TYPE).size());
      for (final LongRange range0 : partitionManager.getRangeSet(EVAL0, DATA_TYPE)) {
        assertEquals(range0, INITIAL_RANGE1);
      }

    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Mocked message sender that can verify the correct input messages are sent.
   */
  final class MockedMsgSender implements ElasticMemoryMsgSender {

    private MockedMsgSender() {
      for (int i = 0; i < NUM_REQUESTS; i++) {
        updateMsgCount[i] = new AtomicInteger(0);
      }
    }

    /**
     * To check the update message is sent in order (receiver first, sender next).
     */
    private final AtomicInteger[] updateMsgCount = new AtomicInteger[NUM_REQUESTS];

    @Override
    public void sendRemoteOpMsg(final String origId, final String destId, final DataOpType operationType,
                                final String dataType, final List<KeyRange> dataKeyRanges,
                                final List<KeyValuePair> dataKVPairList, final String operationId,
                                @Nullable final TraceInfo parentTraceInfo) {

    }

    @Override
    public void sendRemoteOpMsg(final String origId, final String destId, final DataOpType operationType,
                                final String dataType, final DataKey dataKey, final DataValue dataValue,
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

    }

    /**
     * Check the operation id and range matches.
     */
    @Override
    public void sendCtrlMsg(final String destId, final String dataType, final String targetEvalId,
                            final Set<LongRange> idRangeSet, final String operationId,
                            @Nullable final TraceInfo parentTraceInfo) {

      final int index = Integer.valueOf(operationId);
      final String sender = index < 5 ? EVAL0 : EVAL1;
      final String receiver = index < 5 ? EVAL1 : EVAL0;

      assertEquals(sender, destId);
      assertEquals(receiver, targetEvalId);
      assertEquals(DATA_TYPE, dataType);
      assertTrue(idRangeSet.contains(new LongRange(2 * index, 2 * index + 1)));
    }

    @Override
    public void sendCtrlMsg(final String destId, final String dataType, final String targetEvalId, final int numUnits,
                            final String operationId, @Nullable final TraceInfo parentTraceInfo) {
    }

    @Override
    public void sendCtrlMsg(final String destId, final String dataType, final String targetEvalId,
                            final List<Integer> blocks, final String operationId,
                            @Nullable final TraceInfo parentTraceInfo) {

    }

    /**
     * Do nothing because this is called by Evaluator.
     */
    @Override
    public void sendDataMsg(final String destId, final String dataType, final List<UnitIdPair> unitIdPairList,
                            final int blockId, final String operationId, @Nullable final TraceInfo parentTraceInfo) {
    }

    /**
     * Do nothing because this is called by Evaluator.
     */
    @Override
    public void sendDataAckMsg(final Set<LongRange> idRangeSet, final String operationId,
                               @Nullable final TraceInfo parentTraceInfo) {
    }

    /**
     * Do nothing because this is called by Evaluator.
     */
    @Override
    public void sendRegisMsg(final String dataType, final long unitStartId, final long unitEndId,
                             @Nullable final TraceInfo parentTraceInfo) {
    }

    /**
     * Check the update messages are sent in order. i.e., receiver first, and sender.next.
     */
    @Override
    public void sendUpdateMsg(final String destId, final String operationId,
                              @Nullable final TraceInfo parentTraceInfo) {

      final int index = Integer.valueOf(operationId);
      final String sender = index < 5 ? EVAL0 : EVAL1;
      final String receiver = index < 5 ? EVAL1 : EVAL0;

      // We send the update message to the receiver first, and the sender.
      if (updateMsgCount[index].incrementAndGet() == 1) {
        assertEquals(receiver, destId);
      } else if (updateMsgCount[index].incrementAndGet() == 2) {
        assertEquals(sender, destId);
      }
    }

    /**
     * Do nothing because this is called by Evaluator.
     */
    @Override
    public void sendUpdateAckMsg(final String operationId, final UpdateResult result,
                                 @Nullable final TraceInfo parentTraceInfo) {
    }

    @Override
    public void sendOwnershipMsg(final Optional<String> destId, final String operationId, final String dataType,
                                 final int blockId, final int oldOwnerId, final int newOwnerId,
                                 @Nullable final TraceInfo parentTraceInfo) {

    }

    @Override
    public void sendOwnershipAckMsg(final String operationId, final String dataType, final int blockId,
                                    @Nullable final TraceInfo parentTraceInfo) {

    }

    /**
     * Do nothing because this is called by Evaluator.
     */
    @Override
    public void sendFailureMsg(final String operationId, final String reason,
                               @Nullable final TraceInfo parentTraceInfo) {
    }
  }
}
