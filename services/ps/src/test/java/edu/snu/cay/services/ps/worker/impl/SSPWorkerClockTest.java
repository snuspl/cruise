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
package edu.snu.cay.services.ps.worker.impl;

import edu.snu.cay.common.aggregation.avro.AggregationMessage;
import edu.snu.cay.common.aggregation.driver.AggregationMaster;
import edu.snu.cay.common.aggregation.slave.AggregationSlave;
import edu.snu.cay.services.ps.avro.AvroClockMsg;
import edu.snu.cay.services.ps.avro.ClockMsgType;
import edu.snu.cay.services.ps.driver.impl.ClockManager;
import edu.snu.cay.services.ps.ns.ClockMsgCodec;
import edu.snu.cay.services.ps.worker.parameters.StalenessBound;
import edu.snu.cay.utils.ThreadUtils;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.util.StringIdentifierFactory;
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
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link SSPWorkerClock}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({AggregationSlave.class, AggregationMaster.class})
public class SSPWorkerClockTest {
  private static final int STALENESS_BOUND = 4;
  private static final int INIT_GLOBAL_MIN_CLOCK = 10;
  private static final int INIT_WORKER_CLOCK = INIT_GLOBAL_MIN_CLOCK;

  private AggregationSlave mockAggregationSlave;
  private AggregationMaster mockAggregationMaster;
  private ClockMsgCodec codec;
  private SSPWorkerClock sspWorkerClock;
  private SSPWorkerClock.MessageHandler sspWorkerClockMessageHandler;
  private AtomicInteger numberOfTickMsgCalls;

  @Before
  public void setup() throws InjectionException {
    final Configuration conf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(StalenessBound.class, Integer.toString(STALENESS_BOUND))
        .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
        .build();
    final Injector injector = Tang.Factory.getTang().newInjector(conf);
    mockAggregationSlave = mock(AggregationSlave.class);
    injector.bindVolatileInstance(AggregationSlave.class, mockAggregationSlave);
    mockAggregationMaster = mock(AggregationMaster.class);
    injector.bindVolatileInstance(AggregationMaster.class, mockAggregationMaster);

    this.sspWorkerClock = injector.getInstance(SSPWorkerClock.class);
    this.sspWorkerClockMessageHandler = injector.getInstance(SSPWorkerClock.MessageHandler.class);
    this.codec = injector.getInstance(ClockMsgCodec.class);
    this.numberOfTickMsgCalls = new AtomicInteger(0);

    doAnswer(invocation -> {
        final byte[] data = invocation.getArgumentAt(1, byte[].class);
        final AvroClockMsg sendMsg = codec.decode(data);

        if (sendMsg.getType() == ClockMsgType.RequestInitClockMsg) {
          final AvroClockMsg initClockMsg =
              ClockManager.getReplyInitialClockMessage(INIT_GLOBAL_MIN_CLOCK, INIT_WORKER_CLOCK);
          final byte[] replyData = codec.encode(initClockMsg);
          final AggregationMessage aggregationMessage = getTestAggregationMessage("worker", replyData);
          sspWorkerClockMessageHandler.onNext(aggregationMessage);
        } else if (sendMsg.getType() == ClockMsgType.TickMsg) {
          numberOfTickMsgCalls.incrementAndGet();
        }
        return null;
      }).when(mockAggregationSlave).send(anyString(), anyObject());
  }

  /**
   * Tests whether initialize() sets worker clock and global minimum clock according to the reply message.
   * Tests whether clock() ticks its worker clock and sends TICK message.
   */
  @Test
  public void testInitializeAndClock() {
    final int numberOfClockCalls = 3;

    sspWorkerClock.initialize();
    assertEquals(INIT_WORKER_CLOCK, sspWorkerClock.getWorkerClock());
    assertEquals(INIT_GLOBAL_MIN_CLOCK, sspWorkerClock.getGlobalMinimumClock());

    // call clock()
    for (int i = 0; i < numberOfClockCalls; i++) {
      assertEquals(INIT_WORKER_CLOCK + i, sspWorkerClock.getWorkerClock());
      sspWorkerClock.clock();
    }

    assertEquals(numberOfClockCalls, numberOfTickMsgCalls.intValue());
    assertEquals(INIT_WORKER_CLOCK + numberOfClockCalls, sspWorkerClock.getWorkerClock());
    assertEquals(INIT_GLOBAL_MIN_CLOCK, sspWorkerClock.getGlobalMinimumClock());
  }

  /**
   * Tests whether SSPWorkerClock handles BROADCAST_GLOBAL_MINIMUM_CLOCK and updates the global minimum clock.
   */
  @Test
  public void testUpdateGlobalMinimumClock() throws NetworkException {
    final int updatedGlobalMinimumClock = 100;

    sspWorkerClock.initialize();
    assertEquals(INIT_WORKER_CLOCK, sspWorkerClock.getWorkerClock());
    assertEquals(INIT_GLOBAL_MIN_CLOCK, sspWorkerClock.getGlobalMinimumClock());

    doAnswer(invocation -> {
        final byte[] data = invocation.getArgumentAt(2, byte[].class);
        final AggregationMessage aggregationMessage = getTestAggregationMessage("worker", data);
        sspWorkerClockMessageHandler.onNext(aggregationMessage);
        return null;
      }).when(mockAggregationMaster).send(anyString(), anyString(), anyObject());

    // broadcast updated global minimum clock
    final byte[] data =
        codec.encode(ClockManager.getBroadcastMinClockMessage(updatedGlobalMinimumClock));
    mockAggregationMaster.send(ClockManager.AGGREGATION_CLIENT_NAME, "worker", data);

    assertEquals(updatedGlobalMinimumClock, sspWorkerClock.getGlobalMinimumClock());
  }

  /**
   * Tests whether waitIfExceedingStalenessBound() waits if the worker clock exceeds the staleness bound.
   */
  @Test(timeout = 20000)
  public void testWaitIfExceedingStalenessBound() throws InterruptedException, BrokenBarrierException {
    int globalMinimumClock = INIT_GLOBAL_MIN_CLOCK;
    final int numOfThreads = 3;
    final int timeoutInMilliseconds = 1000;
    final CyclicBarrier barrier = new CyclicBarrier(numOfThreads + 1);
    final Runnable[] threads = new Runnable[numOfThreads];

    // a definition of thread waiting at a cyclic barrier after returning from waitIfExceedingStalenessBound().
    final class WaitIfExceedingStalenessBoundThread implements Runnable {
      private final CyclicBarrier cyclicBarrier;

      WaitIfExceedingStalenessBoundThread(final CyclicBarrier cyclicBarrier) {
        this.cyclicBarrier = cyclicBarrier;
      }

      @Override
      public void run() {
        sspWorkerClock.waitIfExceedingStalenessBound();
        try {
          cyclicBarrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
          fail("Exception occurred while waiting: " + e.getMessage());
        }
      }
    }

    // initialize the worker clock and the global minimum clock
    // with INIT_WORKER_CLOCK and INIT_GLOBAL_MIN_CLOCK, respectively.
    sspWorkerClock.initialize();

    // step 1: check that waitIfExceedingStalenessBound() returns immediately
    // when the worker clock is within staleness bound.
    while (sspWorkerClock.getWorkerClock() <= globalMinimumClock + STALENESS_BOUND) {
      for (int i = 0; i < numOfThreads; i++) {
        threads[i] = new WaitIfExceedingStalenessBoundThread(barrier);
      }
      ThreadUtils.runConcurrently(threads);

      // check whether threads return from waitIfExceedingStalenessBound() immediately.
      barrier.await();
      sspWorkerClock.clock();
    }

    // step 2: check that thread is blocked in waitIfExceedingStalenessBound()
    // when the worker clock is outside the staleness bound.
    sspWorkerClock.clock(); // the worker clock == the global minimum clock + (STALENESS_BOUND + 2)
    assertTrue(sspWorkerClock.getWorkerClock() > globalMinimumClock + STALENESS_BOUND);
    for (int i = 0; i < numOfThreads; i++) {
      threads[i] = new WaitIfExceedingStalenessBoundThread(barrier);
    }
    ThreadUtils.runConcurrently(threads);

    // since threads are now blocked, there should be no thread waiting at the barrier.
    Thread.sleep(timeoutInMilliseconds);
    assertEquals(0, barrier.getNumberWaiting());

    // step 3: check that threads keep being blocked
    // since the worker clock is still beyond staleness bound even though global minimum clock is ticked.
    globalMinimumClock++; // the worker clock == the global minimum clock + (STALENESS_BOUND + 1)
    assertTrue(sspWorkerClock.getWorkerClock() > globalMinimumClock + STALENESS_BOUND);

    // send message with an increased global minimum clock
    final byte[] broadcastClockMsgToKeepWait =
        codec.encode(ClockManager.getBroadcastMinClockMessage(globalMinimumClock));
    sspWorkerClockMessageHandler.onNext(getTestAggregationMessage("worker", broadcastClockMsgToKeepWait));

    // since threads are still blocked, there should be no thread waiting at the barrier.
    Thread.sleep(timeoutInMilliseconds);
    assertEquals(0, barrier.getNumberWaiting());

    // step 4: check that threads are released properly
    // when the global minimum clock is increased and the worker clock gets into staleness bound.
    globalMinimumClock++; // the worker clock == the global minimum clock + STALENESS_BOUND
    assertTrue(sspWorkerClock.getWorkerClock() <= globalMinimumClock + STALENESS_BOUND);

    // send message with an increased global minimum clock
    final byte[] broadcastClockMsgToTerminate =
        codec.encode(ClockManager.getBroadcastMinClockMessage(globalMinimumClock));
    sspWorkerClockMessageHandler.onNext(getTestAggregationMessage("worker", broadcastClockMsgToTerminate));

    // waitIfExceedingStalenessBound() is returned finally
    barrier.await();
  }

  private AggregationMessage getTestAggregationMessage(final String workerId, final byte[] data) {
    return AggregationMessage.newBuilder()
        .setSourceId(workerId)
        .setClientClassName(ClockManager.AGGREGATION_CLIENT_NAME)
        .setData(ByteBuffer.wrap(data))
        .build();
  }
}
