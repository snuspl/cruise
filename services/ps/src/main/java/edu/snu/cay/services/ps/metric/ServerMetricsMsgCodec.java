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
package edu.snu.cay.services.ps.metric;

import edu.snu.cay.services.ps.metric.avro.ServerMetrics;
import edu.snu.cay.utils.AvroUtils;
import org.apache.reef.wake.remote.Codec;

import javax.inject.Inject;

/**
 * Codec for server-side Metrics Message.
 * Simply uses AvroUtils to encode and decode messages.
 */
public final class ServerMetricsMsgCodec implements Codec<ServerMetrics> {

  @Inject
  private ServerMetricsMsgCodec() {
  }

  @Override
  public byte[] encode(final ServerMetrics msg) {
    return AvroUtils.toBytes(msg, ServerMetrics.class);
  }

  @Override
  public ServerMetrics decode(final byte[] data) {
    return AvroUtils.fromBytes(data, ServerMetrics.class);
  }
}
