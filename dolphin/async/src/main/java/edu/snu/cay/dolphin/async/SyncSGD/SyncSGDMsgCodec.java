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
package edu.snu.cay.dolphin.async.SyncSGD;

import edu.snu.cay.dolphin.async.AvroSyncSGDMsg;
import edu.snu.cay.utils.AvroUtils;
import org.apache.reef.wake.remote.Codec;

import javax.inject.Inject;

/**
 *
 */
public final class SyncSGDMsgCodec implements Codec<AvroSyncSGDMsg> {
  @Inject
  private SyncSGDMsgCodec() {
  }

  @Override
  public byte[] encode(final AvroSyncSGDMsg msg) {
    return AvroUtils.toBytes(msg, AvroSyncSGDMsg.class);
  }

  @Override
  public AvroSyncSGDMsg decode(final byte[] data) {
    return AvroUtils.fromBytes(data, AvroSyncSGDMsg.class);
  }
}
