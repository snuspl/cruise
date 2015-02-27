/**
 * Copyright (C) 2014 Seoul National University
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
package edu.snu.reef.flexion.examples.ml.sub;

import edu.snu.reef.flexion.examples.ml.data.Centroid;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Codec for encoding and decoding a KMeansCentroid List
 */
public final class CentroidListCodec implements Codec<List<Centroid>> {

  @Inject
  public CentroidListCodec() {
  }

  @Override
  public final byte[] encode(final List<Centroid> list) {

    /* This codec does not assume consistent centroid vector sizes(dimensions).
     * Therefore to specify the initial data size,
     * a quick iteration over the input list to compute
     * the sums of vector sizes is required.
     */
    int vectorSizeSum = 0;
    for (final Centroid centroid : list) {
      vectorSizeSum += centroid.vector.size();
    }

    final ByteArrayOutputStream baos = new ByteArrayOutputStream(Integer.SIZE
                                                                 + Integer.SIZE * 2 * list.size()
                                                                 + Double.SIZE * vectorSizeSum);
    try (final DataOutputStream daos = new DataOutputStream(baos)) {
      daos.writeInt(list.size());
      for (final Centroid centroid : list) {
        daos.writeInt(centroid.getClusterId());
        daos.writeInt(centroid.vector.size());

        for (int j = 0; j < centroid.vector.size(); j++) {
          daos.writeDouble(centroid.vector.get(j));
        }

      }

    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }


    return baos.toByteArray();
  }

  @Override
  public final List<Centroid> decode(final byte[] data) {
    final ByteArrayInputStream bais = new ByteArrayInputStream(data);
    final List<Centroid> list = new ArrayList<>();

    try (final DataInputStream dais = new DataInputStream(bais)) {
      final int listSize = dais.readInt();

      for (int i = 0; i < listSize; i++) {
        final int clusterId = dais.readInt();
        final int vectorSize = dais.readInt();
        final Vector vector = new DenseVector(vectorSize);

        for (int j = 0; j < vectorSize; j++) {
          vector.set(j, dais.readDouble());
        }

        list.add(new Centroid(clusterId, vector));
      }

    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }

    return list;
  }
}
