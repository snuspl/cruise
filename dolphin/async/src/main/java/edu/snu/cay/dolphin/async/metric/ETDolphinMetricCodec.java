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
package edu.snu.cay.dolphin.async.metric;

import edu.snu.cay.dolphin.async.metric.avro.DolphinWorkerMetrics;
import edu.snu.cay.utils.AvroUtils;
import org.apache.reef.io.network.impl.StreamingCodec;

import javax.inject.Inject;
import java.io.*;

/**
 * Codec that (de-)serializes the Dolphin-specific metrics.
 */
public final class ETDolphinMetricCodec implements StreamingCodec<DolphinWorkerMetrics> {
  @Inject
  private ETDolphinMetricCodec() {
  }

  @Override
  public void encodeToStream(final DolphinWorkerMetrics dolphinMetrics, final DataOutputStream dos) {
    try {
      final byte[] encoded = AvroUtils.toBytes(dolphinMetrics, DolphinWorkerMetrics.class);
      dos.writeInt(encoded.length);
      dos.write(encoded);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public DolphinWorkerMetrics decodeFromStream(final DataInputStream dis) {
    try {
      final int length = dis.readInt();
      final byte[] buffer = new byte[length];
      dis.readFully(buffer);
      return decode(buffer);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public DolphinWorkerMetrics decode(final byte[] bytes) {
    return AvroUtils.fromBytes(bytes, DolphinWorkerMetrics.class);
  }

  @Override
  public byte[] encode(final DolphinWorkerMetrics dolphinMetrics) {
    return AvroUtils.toBytes(dolphinMetrics, DolphinWorkerMetrics.class);
  }
}
