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
package edu.snu.spl.cruise.services.et.common.impl;

import edu.snu.spl.cruise.services.et.avro.ETMsg;
import edu.snu.spl.cruise.utils.AvroUtils;
import org.apache.reef.wake.remote.Codec;

import javax.inject.Inject;

/**
 * Codec for ETMsgs.
 * Simply uses AvroUtils to encode and decode messages.
 */
public final class ETMsgCodec implements Codec<ETMsg> {

  @Inject
  private ETMsgCodec() {
  }

  @Override
  public byte[] encode(final ETMsg msg) {
    return AvroUtils.toBytes(msg, ETMsg.class);
  }

  @Override
  public ETMsg decode(final byte[] data) {
    return AvroUtils.fromBytes(data, ETMsg.class);
  }
}
