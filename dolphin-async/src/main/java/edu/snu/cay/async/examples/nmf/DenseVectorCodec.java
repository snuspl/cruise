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
package edu.snu.cay.async.examples.nmf;

import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.common.math.linalg.VectorFactory;
import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;
import java.io.*;
import java.util.logging.Logger;

/**
 * Codec for dense vector.
 * TODO #402: Remove this duplicate of {@code DenseVectorCodec} in dolphin submodule.
 */
public final class DenseVectorCodec implements Codec<Vector>, StreamingCodec<Vector> {
  private static final Logger LOG = Logger.getLogger(DenseVectorCodec.class.getName());
  private final VectorFactory vectorFactory;

  @Inject
  private DenseVectorCodec(final VectorFactory vectorFactory) {
    this.vectorFactory = vectorFactory;
  }

  @Override
  public byte[] encode(final Vector vector) {
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream(getNumBytes(vector));
         final DataOutputStream daos = new DataOutputStream(baos)) {
      encodeToStream(vector, daos);
      return baos.toByteArray();
    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }
  }

  @Override
  public void encodeToStream(final Vector vector, final DataOutputStream daos) {
    if (!vector.isDense()) {
      LOG.warning("the given vector is not dense.");
    }

    try {
      daos.writeInt(vector.length());
      for (int i = 0; i < vector.length(); i++) {
        daos.writeDouble(vector.get(i));
      }
    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }
  }

  @Override
  public Vector decode(final byte[] bytes) {
    try (final DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes))) {
      return decodeFromStream(dis);
    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }
  }

  @Override
  public Vector decodeFromStream(final DataInputStream dais) {
    try {
      final int vecSize = dais.readInt();
      final Vector vector = vectorFactory.createDenseZeros(vecSize);
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
    return Integer.SIZE + Double.SIZE * vector.length();
  }
}
