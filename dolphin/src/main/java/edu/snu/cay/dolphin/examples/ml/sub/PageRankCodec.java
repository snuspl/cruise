/**
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
package edu.snu.cay.dolphin.examples.ml.sub;

import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;
import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Codec for encoding and decoding a list of page rank
 */
public final class PageRankCodec implements Codec<Map<Integer, Double>> {

  @Inject
  public PageRankCodec() {
  }

  @Override
  public final byte[] encode(final Map<Integer, Double> map) {
    final int size = map.size();

    final ByteArrayOutputStream baos = new ByteArrayOutputStream(
      Integer.SIZE + (Integer.SIZE + Double.SIZE) * size);
    try (final DataOutputStream daos = new DataOutputStream(baos)) {
      daos.writeInt(size);
      for (Map.Entry<Integer, Double> entry : map.entrySet()) {
        daos.writeInt(entry.getKey());
        daos.writeDouble(entry.getValue());
      }
    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }

    return baos.toByteArray();
  }

  @Override
  public final Map<Integer, Double> decode(final byte[] data) {
    final ByteArrayInputStream bais = new ByteArrayInputStream(data);
    final Map<Integer, Double> map = new HashMap<>();

    try (final DataInputStream dais = new DataInputStream(bais)) {
      final int size = dais.readInt();

      for (int i = 0; i < size; i++) {
        int nodeId = dais.readInt();
        double nodeRank = dais.readDouble();
        map.put(nodeId, nodeRank);
      }
    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }

    return map;
  }
}
