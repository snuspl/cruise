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
import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;
import java.io.*;
import java.util.logging.Logger;

/**
 * Codec for dense vector.
 */
public final class DenseVectorCodec implements Codec<Vector>, StreamingCodec<Vector> {
  private static final Logger LOG = Logger.getLogger(DenseVectorCodec.class.getName());

  @Inject
  private DenseVectorCodec() {
  }

  @Override
  public byte[] encode(final Vector vector) {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream(getNumBytes(vector));
    final DataOutputStream daos = new DataOutputStream(baos);
    encodeToStream(vector, daos);
    return baos.toByteArray();
  }

  @Override
  public void encodeToStream(final Vector vector, final DataOutputStream daos) {
    if (!vector.isDense()) {
      LOG.warning("the given vector is not dense.");
    }

    try {
      daos.writeInt(vector.size());
      for (int i = 0; i < vector.size(); i++) {
        daos.writeDouble(vector.get(i));
      }
    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }
  }

  @Override
  public Vector decode(final byte[] bytes) {
    final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    final DataInputStream dis = new DataInputStream(bais);
    return decodeFromStream(dis);
  }

  @Override
  public Vector decodeFromStream(final DataInputStream dais) {
    try {
      final int vecSize = dais.readInt();
      final Vector vector = new DenseVector(vecSize);
      for (int i = 0; i < vecSize; i++) {
        vector.set(i, dais.readDouble());
      }
      return vector;
    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }
  }

  public int getNumBytes(final Vector vector) {
    if (!vector.isDense()) {
      LOG.warning("the given vector is not dense.");
    }
    return Integer.SIZE + Double.SIZE * vector.size();
  }
}
