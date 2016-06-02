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
package edu.snu.cay.dolphin.bsp.mlapps.sub;

import edu.snu.cay.dolphin.bsp.mlapps.data.VectorSum;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;
import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Codec for encoding and decoding a map of Integer vs. Vector Sum.
 */
public final class MapOfIntVSumCodec implements Codec<Map<Integer, VectorSum>> {

  @Inject
  public MapOfIntVSumCodec() {
  }

  @Override
  public byte[] encode(final Map<Integer, VectorSum> map) {

    /* This codec does not assume consistent centroid vector sizes(dimensions).
     * Therefore to specify the initial data size,
     * a quick iteration over the input list to compute
     * the sums of vector sizes is required.
     */
    int vectorSizeSum = 0;
    for (final VectorSum vectorSum : map.values()) {
      vectorSizeSum += vectorSum.getSum().size();
    }

    final ByteArrayOutputStream baos =
        new ByteArrayOutputStream(Integer.SIZE
            + Integer.SIZE * 3 * map.size()
            + Double.SIZE * vectorSizeSum);
    try (final DataOutputStream daos = new DataOutputStream(baos)) {
      daos.writeInt(map.size());
      for (final Integer integer : map.keySet()) {
        final VectorSum vectorSum = map.get(integer);
        daos.writeInt(integer);
        daos.writeInt(vectorSum.getSum().size());
        for (int j = 0; j < vectorSum.getSum().size(); j++) {
          daos.writeDouble(vectorSum.getSum().get(j));
        }
        daos.writeInt(vectorSum.getCount());
      }
    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }

    return baos.toByteArray();
  }

  @Override
  public Map<Integer, VectorSum> decode(final byte[] data) {
    final ByteArrayInputStream bais = new ByteArrayInputStream(data);
    final Map<Integer, VectorSum> resultMap = new HashMap<>();

    try (final DataInputStream dais = new DataInputStream(bais)) {
      final int mapSize = dais.readInt();
      for (int i = 0; i < mapSize; i++) {
        final int mapInt = dais.readInt();
        final int vectorSize = dais.readInt();
        final Vector vector = new DenseVector(vectorSize);
        for (int j = 0; j < vectorSize; j++) {
          vector.set(j, dais.readDouble());
        }
        final int count = dais.readInt();
        resultMap.put(mapInt, new VectorSum(vector, count));
      }
    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }

    return resultMap;
  }
}
