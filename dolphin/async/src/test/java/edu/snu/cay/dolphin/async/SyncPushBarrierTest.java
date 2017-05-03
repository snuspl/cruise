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
package edu.snu.cay.dolphin.async;

import edu.snu.cay.common.centcomm.avro.CentCommMsg;
import edu.snu.cay.common.centcomm.master.MasterSideCentCommMsgSender;
import edu.snu.cay.common.centcomm.slave.SlaveSideCentCommMsgSender;
import edu.snu.cay.dolphin.async.SyncSGD.SyncSGDDriverSide.BatchManager;
import edu.snu.cay.dolphin.async.SyncSGD.SyncSGDMsgCodec;
import edu.snu.cay.dolphin.async.SyncSGD.SyncSGDWorkerSide.api.PushBarrier;
import edu.snu.cay.dolphin.async.SyncSGD.SyncSGDWorkerSide.impl.EvalSideSyncMsgHandler;
import edu.snu.cay.dolphin.async.SyncSGD.SyncSGDWorkerSide.impl.SyncPushBarrier;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.nio.ByteBuffer;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link PushBarrier}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({MasterSideCentCommMsgSender.class, SlaveSideCentCommMsgSender.class})
public final class SyncPushBarrierTest {
  private MasterSideCentCommMsgSender masterSideCentCommMsgSender;
  private SyncPushBarrier syncPushBarrier;
  private EvalSideSyncMsgHandler evalSideSyncMsgHandler;
  private SyncSGDMsgCodec codec;

  @Before
  public void setup() throws InjectionException, NetworkException {
    final Configuration conf = Tang.Factory.getTang().newConfigurationBuilder()
        .build();
    final Injector injector = Tang.Factory.getTang().newInjector(conf);
    final SlaveSideCentCommMsgSender slaveSideCentCommMsgSender = mock(SlaveSideCentCommMsgSender.class);
    injector.bindVolatileInstance(SlaveSideCentCommMsgSender.class, slaveSideCentCommMsgSender);
    masterSideCentCommMsgSender = mock(MasterSideCentCommMsgSender.class);
    injector.bindVolatileInstance(MasterSideCentCommMsgSender.class, masterSideCentCommMsgSender);

    this.syncPushBarrier = injector.getInstance(SyncPushBarrier.class);
    this.evalSideSyncMsgHandler = injector.getInstance(EvalSideSyncMsgHandler.class);
    this.codec = injector.getInstance(SyncSGDMsgCodec.class);

    doAnswer(invocation -> {
      final byte[] data = invocation.getArgumentAt(2, byte[].class);
      evalSideSyncMsgHandler.onNext(getTestAggregationMessage("driver", data));
      return null;
    }).when(masterSideCentCommMsgSender).send(anyString(), anyString(), anyObject());
  }

  @Test
  public void testPermitPush() throws InjectionException, NetworkException {
    assert syncPushBarrier.getLatchCount() == 1;
    final PermitPushMsg permitPushMsg = PermitPushMsg.newBuilder().build();
    final AvroSyncSGDMsg avroSyncSGDMsg = AvroSyncSGDMsg.newBuilder()
        .setType(SyncSGDMsgType.PermitPushMsg)
        .setPermitPushMsg(permitPushMsg)
        .build();
    final byte[] data = codec.encode(avroSyncSGDMsg);
    masterSideCentCommMsgSender.send("PushBarrierProtocol", "worker", data);
    assert syncPushBarrier.getLatchCount() == 0;
  }

  @Test
  public void testStartNextMiniBatch() throws InjectionException, NetworkException {
    assert syncPushBarrier.getThisRoundNum() == 0;
    final StartNextMiniBatchMsg startNextMiniBatchMsg = StartNextMiniBatchMsg.newBuilder()
        .setNextRoundNum(1)
        .build();
    final AvroSyncSGDMsg avroSyncSGDMsg = AvroSyncSGDMsg.newBuilder()
        .setType(SyncSGDMsgType.StartNextMiniBatchMsg)
        .setStartNextMiniBatchMsg(startNextMiniBatchMsg)
        .build();
    final byte[] data = codec.encode(avroSyncSGDMsg);
    masterSideCentCommMsgSender.send("PushBarrierProtocol", "worker", data);
    assert syncPushBarrier.getThisRoundNum() == 1;
  }

  private CentCommMsg getTestAggregationMessage(final String driverId, final byte[] data) {
    return CentCommMsg.newBuilder()
        .setSourceId(driverId)
        .setClientClassName(BatchManager.CENT_COMM_CLIENT_NAME)
        .setData(ByteBuffer.wrap(data))
        .build();
  }
}
