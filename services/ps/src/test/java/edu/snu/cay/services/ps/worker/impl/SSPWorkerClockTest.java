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
import edu.snu.cay.services.ps.driver.impl.ClockManager;
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
  private static final int STALENESS = 4;

  private Injector injector;
  private AggregationSlave mockAggregationSlave;
  private AggregationMaster mockAggregationMaster;
  private SerializableCodec<String> codec;
  private SSPWorkerClock sspWorkerClock;
  private SSPWorkerClock.MessageHandler sspWorkerClockMessageHandler;

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

    this.sspWorkerClock = injector.getInstance(SSPWorkerClock.class);
    this.sspWorkerClockMessageHandler = injector.getInstance(SSPWorkerClock.MessageHandler.class);
    this.codec = injector.getInstance(SerializableCodec.class);
  }

  /**
   * Test whether initialize() sets worker clock and global minimum clock according to the reply message.
   * Test whether clock() ticks its worker clock and sends TICK message.
   */
  @Test
  public void testInitializeAndClock() {
    final int initialWorkerClock = 10;
    final int initialGlobalMinimumClock = 30;
    final int numberOFClockCalls = 3;
    final ResettingCountDownLatch countDownLatch = new ResettingCountDownLatch(numberOFClockCalls);

    doAnswer(invocation -> {
        final byte[] data = invocation.getArgumentAt(1, byte[].class);
        final String sendMsg = codec.decode(data);

        if (sendMsg.contains(ClockManager.REQUEST_INITIAL_CLOCK)) {
          final String initClockMsg =
              ClockManager.getInitialClockMessage(initialGlobalMinimumClock, initialWorkerClock);
          final byte[] replyData = codec.encode(initClockMsg);
          final AggregationMessage aggregationMessage = getTestAggregationMessage("worker", replyData);
          sspWorkerClockMessageHandler.onNext(aggregationMessage);
        } else if (sendMsg.contains(ClockManager.TICK)) {
          countDownLatch.countDown();
        }
        return null;
      }).when(mockAggregationSlave).send(anyString(), anyObject());

    sspWorkerClock.initialize();
    assertEquals(initialWorkerClock, sspWorkerClock.getWorkerClock());
    assertEquals(initialGlobalMinimumClock, sspWorkerClock.getGlobalMinimumClock());

    // call clock()
    for (int i = 0; i < numberOFClockCalls; i++) {
      assertEquals(initialWorkerClock + i, sspWorkerClock.getWorkerClock());
      sspWorkerClock.clock();
    }
    countDownLatch.await();
    assertEquals(initialWorkerClock + numberOFClockCalls, sspWorkerClock.getWorkerClock());
    assertEquals(initialGlobalMinimumClock, sspWorkerClock.getGlobalMinimumClock());
  }

  /**
   * Test whether SSPWorkerClock handles BROADCAST_GLOBAL_MINIMUM_CLOCK and updates the global minimum clock.
   */
  @Test
  public void testUpdateGlobalMinimumClock() throws NetworkException {
    final int initialWorkerClock = 10;
    final int initialGlobalMinimumClock = 30;
    final int updatedGlobalMinimumClock = 100;

    doAnswer(invocation -> {
        final byte[] data = invocation.getArgumentAt(1, byte[].class);
        final String sendMsg = codec.decode(data);

        if (sendMsg.contains(ClockManager.REQUEST_INITIAL_CLOCK)) {
          final String initClockMsg =
              ClockManager.getInitialClockMessage(initialGlobalMinimumClock, initialWorkerClock);
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
        codec.encode(ClockManager.getBroadcastGlobalMinimumClockMessage(updatedGlobalMinimumClock));
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
