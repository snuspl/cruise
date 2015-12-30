/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.cay.services.ps.ns;

import edu.snu.cay.services.ps.avro.AvroParameterServerMsg;
import edu.snu.cay.utils.AvroUtils;
import org.apache.reef.wake.remote.Codec;

import javax.inject.Inject;

/**
 * Codec for {@link AvroParameterServerMsg}s.
 * Simply uses {@link AvroUtils} to encode and decode messages.
 */
public final class ParameterServerMsgCodec implements Codec<AvroParameterServerMsg> {

  @Inject
  private ParameterServerMsgCodec() {
  }

  @Override
  public byte[] encode(final AvroParameterServerMsg msg) {
    return AvroUtils.toBytes(msg, AvroParameterServerMsg.class);
  }

  @Override
  public AvroParameterServerMsg decode(final byte[] data) {
    return AvroUtils.fromBytes(data, AvroParameterServerMsg.class);
  }
}
