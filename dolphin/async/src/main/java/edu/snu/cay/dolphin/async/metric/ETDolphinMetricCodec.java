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

import edu.snu.cay.dolphin.async.metric.avro.DolphinMetrics;
import edu.snu.cay.utils.AvroUtils;
import org.apache.reef.io.network.impl.StreamingCodec;

import javax.inject.Inject;
import java.io.*;

/**
 * Codec that (de-)serializes the Dolphin-specific metrics.
 */
public class ETDolphinMetricCodec implements StreamingCodec<DolphinMetrics> {
  @Inject
  private ETDolphinMetricCodec() {
  }

  @Override
  public void encodeToStream(final DolphinMetrics dolphinMetrics, final DataOutputStream dos) {
    try {
      final byte[] encoded = AvroUtils.toBytes(dolphinMetrics, DolphinMetrics.class);
      dos.writeInt(encoded.length);
      dos.write(encoded);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public DolphinMetrics decodeFromStream(final DataInputStream dis) {
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
  public DolphinMetrics decode(final byte[] bytes) {
    return AvroUtils.fromBytes(bytes, DolphinMetrics.class);
  }

  @Override
  public byte[] encode(final DolphinMetrics dolphinMetrics) {
    return AvroUtils.toBytes(dolphinMetrics, DolphinMetrics.class);
  }
}
