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
package edu.snu.cay.dolphin.core.metric;

import javax.inject.Inject;
import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Default implementation of MetricCodec
 */
public final class DefaultMetricCodecImpl implements MetricCodec {

  @Inject
  public DefaultMetricCodecImpl() {
  }

  @Override
  public byte[] encode(final Map<String, Double> map) {
    try (final ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
         final ObjectOutputStream out = new ObjectOutputStream(byteOut)) {
      out.writeInt(map.size());
      for (final Map.Entry<String, Double> entry : map.entrySet()) {
        out.writeUTF(entry.getKey());
        out.writeDouble(entry.getValue());
      }
      out.flush();
      byteOut.flush();
      return byteOut.toByteArray();
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Map<String, Double> decode(final byte[] data) {
    try (final ByteArrayInputStream byteIn = new ByteArrayInputStream(data);
         final ObjectInputStream in = new ObjectInputStream(byteIn)) {
      final Map<String, Double> result = new HashMap<>();
      final int count = in.readInt();
      for (int i = 0; i < count; i++) {
        final String key = in.readUTF();
        final Double value = in.readDouble();
        result.put(key, value);
      }
      return result;
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
