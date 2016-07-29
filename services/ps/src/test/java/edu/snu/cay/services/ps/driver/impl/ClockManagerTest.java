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

import edu.snu.cay.common.aggregation.avro.AggregationMessage;
import edu.snu.cay.common.aggregation.driver.AggregationMaster;
import edu.snu.cay.common.aggregation.slave.AggregationSlave;
import edu.snu.cay.services.ps.worker.parameters.Staleness;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.group.impl.utils.ResettingCountDownLatch;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.IdentifierFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link ClockManager}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({AggregationSlave.class, AggregationMaster.class})
public final class ClockManagerTest {
  private static final int STALENESS = 4;
  private static final int NUM_WORKERS = 10;

  private Injector injector;

  private AggregationSlave mockAggregationSlave;
  private AggregationMaster mockAggregationMaster;
  private ClockManager clockManager;
  private ClockManager.MessageHandler clockMessageHandler;
  private SerializableCodec<String> codec;

  @Before
  public void setup() throws InjectionException {
    final Configuration conf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(Staleness.class, Integer.toString(STALENESS))
        .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
        .build();
    injector = Tang.Factory.getTang().newInjector(conf);
    mockAggregationSlave = mock(AggregationSlave.class);
    injector.bindVolatileInstance(AggregationSlave.class, mockAggregationSlave);
    mockAggregationMaster = mock(AggregationMaster.class);
    injector.bindVolatileInstance(AggregationMaster.class, mockAggregationMaster);

    this.clockManager = injector.getInstance(ClockManager.class);
    this.clockMessageHandler = injector.getInstance(ClockManager.MessageHandler.class);
    this.codec = injector.getInstance(SerializableCodec.class);
  }

  /**
   * Test whether ClockManager initializes the clock of workers not added by EM with current global minimum clock.
   * Test whether ClockManager initializes the clock of workers added by EM with global minimum clock + (staleness /2).
   */
  @Test
  public void testInitializingWorkers() throws InjectionException, NetworkException {
    final int initialGlobalMinimumClock = clockManager.getGlobalMinimumClock();
    final int expectedClockOfWorkersAddedByEM = clockManager.getGlobalMinimumClock() + (STALENESS / 2);
    final int numEarlyWorkers = NUM_WORKERS / 2;
    final ResettingCountDownLatch countDownLatch = new ResettingCountDownLatch(numEarlyWorkers);

    doAnswer(invocation -> {
        final String workerId = invocation.getArgumentAt(0, String.class);
        final byte[] data = invocation.getArgumentAt(1, byte[].class);
        final AggregationMessage aggregationMessage = getTestAggregationMessage(workerId, data);

        clockMessageHandler.onNext(aggregationMessage);
        countDownLatch.countDown();
        return null;
      }).when(mockAggregationSlave).send(anyString(), anyObject());

    // initialize the clock of workers not added by EM
    for (int workerIdx = 0; workerIdx < numEarlyWorkers; workerIdx++) {
      final String workerId = Integer.toString(workerIdx);
      clockManager.onWorkerAdded(false, workerId);

      final byte[] data = codec.encode(ClockManager.REQUEST_INITIAL_CLOCK);
      mockAggregationSlave.send(workerId, data);

      // new clock of worker which is not added by EM equals to globalMinimumClock;
      assertEquals(clockManager.getGlobalMinimumClock(), clockManager.getClockOf(workerId).intValue());
    }

    countDownLatch.awaitAndReset(NUM_WORKERS - numEarlyWorkers);
    // new clock of worker not added by EM do not change global minimum clock
    assertEquals(initialGlobalMinimumClock, clockManager.getGlobalMinimumClock());

    // initialize the clock of workers added by EM
    for (int workerIdx = numEarlyWorkers; workerIdx < NUM_WORKERS; workerIdx++) {
      final String workerId = Integer.toString(workerIdx);
      clockManager.onWorkerAdded(true, workerId);

      final byte[] data = codec.encode(ClockManager.REQUEST_INITIAL_CLOCK);
      mockAggregationSlave.send(workerId, data);

      // new clock of worker which is added by EM is globalMinimumClock + staleness / 2 ;
      assertEquals(expectedClockOfWorkersAddedByEM, clockManager.getClockOf(workerId).intValue());
    }

    countDownLatch.await();
    // new workers added by EM do not change global minimum clock
    assertEquals(initialGlobalMinimumClock, clockManager.getGlobalMinimumClock());
  }

  /**
   * Test whether global minimum clock is updated when the minimum clock worker is deleted.
   */
  @Test
  public void testDeletionOfMinimumWorkers() {
    final int initialGlobalMinimumClock = clockManager.getGlobalMinimumClock();

    doAnswer(invocation -> {
        final String workerId = invocation.getArgumentAt(0, String.class);
        final byte[] data = invocation.getArgumentAt(1, byte[].class);
        final AggregationMessage aggregationMessage = getTestAggregationMessage(workerId, data);

        clockMessageHandler.onNext(aggregationMessage);
        return null;
      }).when(mockAggregationSlave).send(anyString(), anyObject());

    // add workers(not added by EM)
    for (int workerIdx = 0; workerIdx < NUM_WORKERS; workerIdx++) {
      final String workerId = Integer.toString(workerIdx);
      clockManager.onWorkerAdded(false, workerId);
    }

    for (int workerIdx = 0; workerIdx < NUM_WORKERS; workerIdx++) {
      final String workerId = Integer.toString(workerIdx);
      // tick worker id times
      for (int i = 0; i < workerIdx; i++) {
        final byte[] data = codec.encode(ClockManager.TICK);
        mockAggregationSlave.send(workerId, data);
      }
      assertEquals(initialGlobalMinimumClock + workerIdx, clockManager.getClockOf(workerId).intValue());
    }

    // delete minimum clock worker
    // in this test, minimum clock worker has minimum worker id
    for (int workerIdx = 0; workerIdx < NUM_WORKERS; workerIdx++) {
      assertEquals(initialGlobalMinimumClock + workerIdx, clockManager.getGlobalMinimumClock());

      final String workerId = Integer.toString(workerIdx);
      clockManager.onWorkerDeleted(workerId);
    }

    // if there is no worker, minimum clock is same as initial global minimum clock
    assertEquals(initialGlobalMinimumClock, clockManager.getGlobalMinimumClock());
  }

  /**
   * Test whether clock manager broadcasts when minimum global clock is updated.
   */
  @Test
  public void testBroadcasting() throws NetworkException {
    final int initialGlobalMinimumClock = clockManager.getGlobalMinimumClock();
    final Map<String, Integer> workerClockMap = new HashMap<>();
    // check whether the number of global minimum updates is same with the number of broadcast messages
    // that are sent from ClockManager
    // each broadcast message is sent to all workers,
    // so the total message count is numberOfMinClockUpdates(=NUM_WORKERS) * NUM_WORKERS
    final ResettingCountDownLatch countDownLatch = new ResettingCountDownLatch(NUM_WORKERS * NUM_WORKERS);
    int numberOfMinClockUpdates = 0;

    doAnswer(invocation -> {
        final String workerId = invocation.getArgumentAt(0, String.class);
        final byte[] data = invocation.getArgumentAt(1, byte[].class);
        final AggregationMessage aggregationMessage = getTestAggregationMessage(workerId, data);

        clockMessageHandler.onNext(aggregationMessage);
        return null;
      }).when(mockAggregationSlave).send(anyString(), anyObject());

    doAnswer(invocation -> {
        final byte[] data = invocation.getArgumentAt(2, byte[].class);
        final String sendMsg = codec.decode(data);

        if (sendMsg.contains(ClockManager.BROADCAST_GLOBAL_MINIMUM_CLOCK)) {
          // check broadcast count is same as number of minimum clock updates
          countDownLatch.countDown();
        }
        return null;
      }).when(mockAggregationMaster).send(anyString(), anyString(), anyObject());

    // Add workers first to set same initial clock to all workers
    for (int workerIdx = 0; workerIdx < NUM_WORKERS; workerIdx++) {
      final String workerId = Integer.toString(workerIdx);
      clockManager.onWorkerAdded(false, workerId);
    }

    for (int workerIdx = 0; workerIdx < NUM_WORKERS; workerIdx++) {
      final String workerId = Integer.toString(workerIdx);
      // tick clock its worker id times
      for (int i = 0; i < workerIdx; i++) {
        final byte[] data = codec.encode(ClockManager.TICK);
        mockAggregationSlave.send(workerId, data);
      }
      workerClockMap.put(workerId, initialGlobalMinimumClock + workerIdx);
    }

    for (int workerIdx = 0; workerIdx < NUM_WORKERS; workerIdx++) {

      // tick the minimum worker clocks
      for (int i = 0; i <= workerIdx; i++) {
        final String workerId = Integer.toString(i);
        final int currentClock = workerClockMap.get(workerId);
        final byte[] data = codec.encode(ClockManager.TICK);
        mockAggregationSlave.send(workerId, data);
        workerClockMap.put(workerId, currentClock + 1);
      }

      // minimum clock is changed because all the minimum clocks are ticked
      numberOfMinClockUpdates++;
    }

    // test if previous assumption(numberOfMinClockUpdates=NUM_WORKERS) is correct
    assertEquals(numberOfMinClockUpdates, NUM_WORKERS);
    countDownLatch.await();

    final int expectedMinimumClock = Collections.min(workerClockMap.values());
    assertEquals(expectedMinimumClock, clockManager.getGlobalMinimumClock());
  }

  private AggregationMessage getTestAggregationMessage(final String workerId, final byte[] data) {
    return AggregationMessage.newBuilder()
        .setSourceId(workerId)
        .setClientClassName(ClockManager.AGGREGATION_CLIENT_NAME)
        .setData(ByteBuffer.wrap(data))
        .build();
  }
}
