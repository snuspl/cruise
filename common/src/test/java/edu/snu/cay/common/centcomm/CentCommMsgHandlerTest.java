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
package edu.snu.cay.common.centcomm;

import edu.snu.cay.common.centcomm.avro.CentCommMsg;
import edu.snu.cay.common.centcomm.ns.CentCommMsgHandler;
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
 * Tests {@link CentCommMsgHandler}.
 * Checks that {@link CentCommMsgHandler} hands over NCS messages to correct client-side handlers.
 */
@Unit
public final class CentCommMsgHandlerTest {
  private static final byte[] DATA_A = new byte[]{0};
  private static final byte[] DATA_B = new byte[]{1};
  private CentCommMsgHandler centCommMsgHandler;

  @Inject
  public CentCommMsgHandlerTest() {
  }

  @Before
  public void setUp() throws InjectionException {
    final Configuration driverConf = CentCommConf.newBuilder()
        .addCentCommClient(MockedMasterMsgHandlerA.class.getName(),
            MockedMasterMsgHandlerA.class,
            MockedSlaveMsgHandlerA.class)
        .addCentCommClient(MockedMasterMsgHandlerB.class.getName(),
            MockedMasterMsgHandlerB.class,
            MockedSlaveMsgHandlerB.class)
        .build()
        .getDriverConfiguration();

    centCommMsgHandler = Tang.Factory.getTang().newInjector(driverConf).getInstance(CentCommMsgHandler.class);
  }

  /**
   * Uses multiple CentComm clients for test.
   */
  @Test
  public void testMultipleCentCommClients() {
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
    centCommMsgHandler.onNext(mockedMessageA);
    centCommMsgHandler.onNext(mockedMessageB);
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

