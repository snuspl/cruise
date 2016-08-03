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
import edu.snu.cay.services.ps.worker.parameters.Staleness;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
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
  private final int staleness = 4;
  private final int initialWorkerClock = 10;
  private final int initialGlobalMinimumClock = 10;

  private AggregationSlave mockAggregationSlave;
  private AggregationMaster mockAggregationMaster;
  private ClockMsgCodec codec;
  private SSPWorkerClock sspWorkerClock;
  private SSPWorkerClock.MessageHandler sspWorkerClockMessageHandler;
  private AtomicInteger numberOfTickMsgCalls;

  @Before
  public void setup() throws InjectionException {
    final Configuration conf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(Staleness.class, Integer.toString(staleness))
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
              ClockManager.getReplyInitialClockMessage(initialGlobalMinimumClock, initialWorkerClock);
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
    assertEquals(initialWorkerClock, sspWorkerClock.getWorkerClock());
    assertEquals(initialGlobalMinimumClock, sspWorkerClock.getGlobalMinimumClock());

    // call clock()
    for (int i = 0; i < numberOfClockCalls; i++) {
      assertEquals(initialWorkerClock + i, sspWorkerClock.getWorkerClock());
      sspWorkerClock.clock();
    }

    assertEquals(numberOfClockCalls, numberOfTickMsgCalls.intValue());
    assertEquals(initialWorkerClock + numberOfClockCalls, sspWorkerClock.getWorkerClock());
    assertEquals(initialGlobalMinimumClock, sspWorkerClock.getGlobalMinimumClock());
  }

  /**
   * Tests whether SSPWorkerClock handles BROADCAST_GLOBAL_MINIMUM_CLOCK and updates the global minimum clock.
   */
  @Test
  public void testUpdateGlobalMinimumClock() throws NetworkException {
    final int updatedGlobalMinimumClock = 100;

    sspWorkerClock.initialize();
    assertEquals(initialWorkerClock, sspWorkerClock.getWorkerClock());
    assertEquals(initialGlobalMinimumClock, sspWorkerClock.getGlobalMinimumClock());

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
  @Test
  public void testWaitIfExceedingStalenessBound() throws InterruptedException {
    int globalMinimumClock = initialGlobalMinimumClock;
    final int numOfThreads = 3;
    final List<Thread> threads = new ArrayList<>();
    class WaitIfExceedingStalenessBoundThread extends Thread {
      @Override
      public void run() {
        try {
          sspWorkerClock.waitIfExceedingStalenessBound();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }

    sspWorkerClock.initialize();

    // test whether waitIfExceedingStalenessBound() returns immediately when the worker clock is in staleness bound
    while (sspWorkerClock.getWorkerClock() <= globalMinimumClock + staleness) {
      for (int i = 0; i < numOfThreads; i++) {
        final Thread thread = new WaitIfExceedingStalenessBoundThread();
        threads.add(thread);
        thread.start();
      }

      // wait until the thread calls waitIfExceedingStalenessBound
      Thread.sleep(500);

      // waitIfExceedingStalenessBound() call is returned immediately,
      // so the thread would terminate at this moment.
      for (final Thread thread : threads) {
        assertEquals(Thread.State.TERMINATED, thread.getState());
      }
      threads.clear();
      sspWorkerClock.clock();
    }

    sspWorkerClock.clock();
    assertTrue(sspWorkerClock.getWorkerClock() > globalMinimumClock + staleness);
    for (int i = 0; i < numOfThreads; i++) {
      final Thread thread = new WaitIfExceedingStalenessBoundThread();
      threads.add(thread);
      thread.start();
    }
    // wait until the thread calls waitIfExceedingStalenessBound
    Thread.sleep(500);

    // waitIfExceedingStalenessBound() call is blocked until the worker clock is within the staleness bound,
    // so the thread would wait at this moment.
    for (final Thread thread : threads) {
      assertEquals(Thread.State.WAITING, thread.getState());
    }

    globalMinimumClock++;
    // the worker clock is out of staleness bound even though global minimum clock is ticked.
    assertTrue(sspWorkerClock.getWorkerClock() > globalMinimumClock + staleness);

    // send message with increased global minimum clock
    final byte[] broadcastClockMsgToKeepWait =
        codec.encode(ClockManager.getBroadcastMinClockMessage(globalMinimumClock));
    sspWorkerClockMessageHandler.onNext(getTestAggregationMessage("worker", broadcastClockMsgToKeepWait));

    // wait until the above message is handled.
    Thread.sleep(500);

    // thread is still waiting because the worker clock is still out of the staleness bound
    for (final Thread thread : threads) {
      assertEquals(Thread.State.WAITING, thread.getState());
    }

    globalMinimumClock++;
    // now, this increased global minimum clock is enough to terminate the thread.
    assertTrue(sspWorkerClock.getWorkerClock() <= globalMinimumClock + staleness);

    // send message with increased global minimum clock
    final byte[] broadcastClockMsgToTerminate =
        codec.encode(ClockManager.getBroadcastMinClockMessage(globalMinimumClock));
    sspWorkerClockMessageHandler.onNext(getTestAggregationMessage("worker", broadcastClockMsgToTerminate));

    // wait until the above message is handled.
    Thread.sleep(500);

    // thread is now terminated because the worker clock is within the staleness bound
    for (final Thread thread : threads) {
      assertEquals(Thread.State.TERMINATED, thread.getState());
    }
  }

  private AggregationMessage getTestAggregationMessage(final String workerId, final byte[] data) {
    return AggregationMessage.newBuilder()
        .setSourceId(workerId)
        .setClientClassName(ClockManager.AGGREGATION_CLIENT_NAME)
        .setData(ByteBuffer.wrap(data))
        .build();
  }
}
