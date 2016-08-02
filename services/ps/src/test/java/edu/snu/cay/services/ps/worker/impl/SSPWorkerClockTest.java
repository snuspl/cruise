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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
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
  }

  /**
   * Tests whether initialize() sets worker clock and global minimum clock according to the reply message.
   * Tests whether clock() ticks its worker clock and sends TICK message.
   */
  @Test
  public void testInitializeAndClock() {
    final int numberOfClockCalls = 3;
    final AtomicInteger numberOfTickMsgCalls = new AtomicInteger(0);

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

    doAnswer(invocation -> {
        final byte[] data = invocation.getArgumentAt(1, byte[].class);
        final AvroClockMsg sendMsg = codec.decode(data);

        if (sendMsg.getType() == ClockMsgType.RequestInitClockMsg) {
          final AvroClockMsg initClockMsg =
              ClockManager.getReplyInitialClockMessage(initialGlobalMinimumClock, initialWorkerClock);
          final byte[] replyData = codec.encode(initClockMsg);
          final AggregationMessage aggregationMessage = getTestAggregationMessage("worker", replyData);
          sspWorkerClockMessageHandler.onNext(aggregationMessage);
        }
        return null;
      }).when(mockAggregationSlave).send(anyString(), anyObject());

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

  private AggregationMessage getTestAggregationMessage(final String workerId, final byte[] data) {
    return AggregationMessage.newBuilder()
        .setSourceId(workerId)
        .setClientClassName(ClockManager.AGGREGATION_CLIENT_NAME)
        .setData(ByteBuffer.wrap(data))
        .build();
  }
}