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
package edu.snu.cay.dolphin.async.SyncSGD.SyncSGDWorkerSide.impl;

import edu.snu.cay.common.centcomm.avro.CentCommMsg;
import edu.snu.cay.dolphin.async.AvroSyncSGDMsg;
import edu.snu.cay.dolphin.async.SyncSGD.SyncSGDMsgCodec;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;

public final class EvalSideSyncMsgHandler implements EventHandler<CentCommMsg> {
  public static final String AGGREGATION_CLIENT_NAME = EvalSideSyncMsgHandler.class.getName();
  private final SyncPushBarrier syncPushBarrier;
  private final SyncSGDMsgCodec codec;
  private final SyncMiniBatchBarrier syncMiniBatchBarrier;

  @Inject
  private EvalSideSyncMsgHandler(final SyncPushBarrier syncPushBarrier,
                                 final SyncSGDMsgCodec syncSGDMsgCodec,
                                 final SyncMiniBatchBarrier syncMiniBatchBarrier) {
    this.syncPushBarrier = syncPushBarrier;
    this.codec = syncSGDMsgCodec;
    this.syncMiniBatchBarrier = syncMiniBatchBarrier;
  }

  @Override
  public void onNext(final CentCommMsg centCommMsg) {
    final AvroSyncSGDMsg avroSyncSGDMsg = codec.decode(centCommMsg.getData().array());
    switch (avroSyncSGDMsg.getType()) {
    case PermitPushMsg:
      syncPushBarrier.countDownPushLatch();
      break;
    case StartNextMiniBatchMsg:
      final int nextRoundNum = avroSyncSGDMsg.getStartNextMiniBatchMsg().getNextRoundNum();
      syncPushBarrier.prepareNextMiniBatch(nextRoundNum);
      syncMiniBatchBarrier.startNextMiniBatch();
      break;
    case TerminateLearningMsg:
      syncMiniBatchBarrier.terminateLearning();
      break;
    default:
      throw new RuntimeException("Unexpected message type: " + avroSyncSGDMsg.getType().toString());
    }
  }
}
