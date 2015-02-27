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

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;
import java.io.*;
import java.util.LinkedList;
import java.util.List;

/**
 * A codec that encodes and decodes a list of Vectors
 */
public final class VectorListCodec implements Codec<List<Vector>> {

  @Inject
  public VectorListCodec() {
  }

  @Override
  public final byte[] encode(final List<Vector> list) {

    /* This codec does not assume consistent centroid vector sizes(dimensions).
     * Therefore to specify the initial data size,
     * a quick iteration over the input list to compute
     * the sums of vector sizes is required.
     */
    int vectorSizeSum = 0;
    for (final Vector vector : list) {
      vectorSizeSum += vector.size();
    }

    final ByteArrayOutputStream baos =
        new ByteArrayOutputStream(Integer.SIZE
                                  + Integer.SIZE * list.size()
                                  + Double.SIZE * vectorSizeSum);
    try (final DataOutputStream daos = new DataOutputStream(baos)) {
      daos.writeInt(list.size());

      for (final Vector vector : list) {
        daos.writeInt(vector.size());

        for (int i = 0; i < vector.size(); i++) {
          daos.writeDouble(vector.get(i));
        }
      }
    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }

    return baos.toByteArray();
  }

  public final List<Vector> decode(final byte[] data) {
    final ByteArrayInputStream bais = new ByteArrayInputStream(data);
    final List<Vector> resultList = new LinkedList<>();

    try (final DataInputStream dais = new DataInputStream(bais)) {
      final int listSize = dais.readInt();

      for (int i = 0; i < listSize; i++) {
        final Vector vector = new DenseVector(dais.readInt());

        for (int j = 0; j < vector.size(); j++) {
          vector.set(j, dais.readDouble());
        }
        resultList.add(vector);
      }

    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }

    return resultList;
  }
}
