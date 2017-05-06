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

import edu.snu.cay.common.centcomm.slave.SlaveSideCentCommMsgSender;
import edu.snu.cay.dolphin.async.AvroSyncSGDMsg;
import edu.snu.cay.dolphin.async.MiniBatchFinishedMsg;
import edu.snu.cay.dolphin.async.RequestPushPermissionMsg;
import edu.snu.cay.dolphin.async.SyncSGD.SyncSGDDriverSide.DriverSideSyncSGDMsgHandler;
import edu.snu.cay.dolphin.async.SyncSGD.SyncSGDMsgCodec;
import edu.snu.cay.dolphin.async.SyncSGDMsgType;

import javax.inject.Inject;

/**
 * Message sender for SyncSGD.
 */
final class WorkerSideSyncSGDMsgSender {
  private final SlaveSideCentCommMsgSender slaveSideCentCommMsgSender;
  private final SyncSGDMsgCodec codec;

  @Inject
  private WorkerSideSyncSGDMsgSender(final SlaveSideCentCommMsgSender slaveSideCentCommMsgSender,
                                     final SyncSGDMsgCodec syncSGDMsgCodec) {
    this.slaveSideCentCommMsgSender = slaveSideCentCommMsgSender;
    this.codec = syncSGDMsgCodec;
  }

  /**
   * Send {@code RequestPushPermissionMsg} to driver.
   * @param thisRoundNum driver distinguishes up-to-date msg and deprecated msg by using this value.
   */
  void sendRequestPushPermissionMsg(final int thisRoundNum) {
    final RequestPushPermissionMsg requestPushPermissionMsg = RequestPushPermissionMsg.newBuilder()
        .setRoundNum(thisRoundNum)
        .build();
    final AvroSyncSGDMsg avroSyncSGDMsg = AvroSyncSGDMsg.newBuilder()
        .setType(SyncSGDMsgType.RequestPushPermissionMsg)
        .setRequestPushPermissionMsg(requestPushPermissionMsg)
        .build();
    final byte[] data = codec.encode(avroSyncSGDMsg);
    slaveSideCentCommMsgSender.send(DriverSideSyncSGDMsgHandler.AGGREGATION_CLIENT_NAME, data);
  }

  /**
   * Send {@code MiniBatchFinishedMsg} to driver.
   * @param epochIdx driver decides whether to progress learning or terminate learning.
   */
  void sendMiniBatchFinishedMsg(final int epochIdx) {
    final MiniBatchFinishedMsg miniBatchFinishedMsg = MiniBatchFinishedMsg.newBuilder()
        .setEpochIdx(epochIdx)
        .build();
    final AvroSyncSGDMsg avroSyncSGDMsg = AvroSyncSGDMsg.newBuilder()
        .setType(SyncSGDMsgType.MiniBatchFinishedMsg)
        .setMiniBatchFinishedMsg(miniBatchFinishedMsg)
        .build();
    final byte[] data = codec.encode(avroSyncSGDMsg);
    slaveSideCentCommMsgSender.send(DriverSideSyncSGDMsgHandler.AGGREGATION_CLIENT_NAME, data);
  }
}
