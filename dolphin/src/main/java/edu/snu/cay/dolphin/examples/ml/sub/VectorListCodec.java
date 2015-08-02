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
package edu.snu.cay.dolphin.examples.ml.sub;

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

    /*
     * This codec assume that vectors have the same length
     */
    int length = 0;
    for (final Vector vector : list) {
      length = vector.size();
    }

    final ByteArrayOutputStream baos =
        new ByteArrayOutputStream(Integer.SIZE
            + Integer.SIZE
            + Double.SIZE * length * list.size());

    try (final DataOutputStream daos = new DataOutputStream(baos)) {
      daos.writeInt(list.size());
      daos.writeInt(length);
      for (final Vector vector : list) {
        for (int i = 0; i < length; i++) {
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
      final int length = dais.readInt();
      for (int i = 0; i < listSize; i++) {
        final Vector vector = new DenseVector(length);
        for (int j = 0; j < length; j++) {
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
