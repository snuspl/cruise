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
package edu.snu.cay.dolphin.async.SyncSGD.SyncSGDDriverSide;

import edu.snu.cay.common.centcomm.avro.CentCommMsg;
import edu.snu.cay.dolphin.async.AvroSyncSGDMsg;
import edu.snu.cay.dolphin.async.SyncSGD.SyncSGDMsgCodec;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;

/**
 * TODO #940: This handler should be implemented for {@link BatchManager}.
 * Handles messages related to SyncSGD from workers.
 */
public final class DriverSideSyncSGDMsgHandler implements EventHandler<CentCommMsg> {
  public static final String AGGREGATION_CLIENT_NAME = DriverSideSyncSGDMsgHandler.class.getName();
  private SyncSGDMsgCodec codec;

  @Inject
  private DriverSideSyncSGDMsgHandler(final SyncSGDMsgCodec syncSGDMsgCodec) {
    this.codec = syncSGDMsgCodec;
  }

  @Override
  public void onNext(final CentCommMsg centCommMsg) {
    final AvroSyncSGDMsg rcvMsg = codec.decode(centCommMsg.getData().array());
    final String workerId = centCommMsg.getSourceId().toString();
    switch (rcvMsg.getType()) {
    case RequestPushPermissionMsg:
      break;
    case MiniBatchFinishedMsg:
      break;
    default:
      throw new RuntimeException("Unexpected message type: " + rcvMsg.getType().toString());
    }
  }
}
