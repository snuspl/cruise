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
package edu.snu.cay.common.aggregation;

import edu.snu.cay.common.aggregation.avro.CentCommMsg;
import edu.snu.cay.common.centcomm.AggregationConfiguration;
import edu.snu.cay.common.centcomm.ns.AggregationMsgHandler;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.impl.NSMessage;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EventHandler;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.inject.Inject;
import java.nio.ByteBuffer;

/**
 * Tests {@link AggregationMsgHandler}.
 * Checks that {@link AggregationMsgHandler} hands over NCS messages to correct client-side handlers.
 */
@Unit
public final class AggregationMsgHandlerTest {
  private static final byte[] DATA_A = new byte[]{0};
  private static final byte[] DATA_B = new byte[]{1};
  private AggregationMsgHandler aggregationMsgHandler;

  @Inject
  public AggregationMsgHandlerTest() {
  }

  @Before
  public void setUp() throws InjectionException {
    final Configuration driverConf = AggregationConfiguration.newBuilder()
        .addAggregationClient(MockedMasterMsgHandlerA.class.getName(),
            MockedMasterMsgHandlerA.class,
            MockedSlaveMsgHandlerA.class)
        .addAggregationClient(MockedMasterMsgHandlerB.class.getName(),
            MockedMasterMsgHandlerB.class,
            MockedSlaveMsgHandlerB.class)
        .build()
        .getDriverConfiguration();

    aggregationMsgHandler = Tang.Factory.getTang().newInjector(driverConf).getInstance(AggregationMsgHandler.class);
  }

  /**
   * Uses multiple aggregation clients for test.
   */
  @Test
  public void testMultipleAggregationClients() {
    final Message<CentCommMsg> mockedMessageA = new NSMessage(null, null, CentCommMsg.newBuilder()
        .setSourceId("")
        .setClientClassName(MockedMasterMsgHandlerA.class.getName())
        .setData(ByteBuffer.wrap(DATA_A))
        .build());
    final Message<CentCommMsg> mockedMessageB = new NSMessage(null, null, CentCommMsg.newBuilder()
        .setSourceId("")
        .setClientClassName(MockedMasterMsgHandlerB.class.getName())
        .setData(ByteBuffer.wrap(DATA_B))
        .build());
    aggregationMsgHandler.onNext(mockedMessageA);
    aggregationMsgHandler.onNext(mockedMessageB);
  }

  final class MockedMasterMsgHandlerA implements EventHandler<CentCommMsg> {

    @Override
    public void onNext(final CentCommMsg message) {
      Assert.assertArrayEquals(message.getData().array(), DATA_A);
    }
  }

  final class MockedMasterMsgHandlerB implements EventHandler<CentCommMsg> {

    @Override
    public void onNext(final CentCommMsg message) {
      Assert.assertArrayEquals(message.getData().array(), DATA_B);
    }
  }

  final class MockedSlaveMsgHandlerA implements EventHandler<CentCommMsg> {

    @Override
    public void onNext(final CentCommMsg message) {
      // do nothing
    }
  }

  final class MockedSlaveMsgHandlerB implements EventHandler<CentCommMsg> {

    @Override
    public void onNext(final CentCommMsg message) {
      // do nothing
    }
  }
}

