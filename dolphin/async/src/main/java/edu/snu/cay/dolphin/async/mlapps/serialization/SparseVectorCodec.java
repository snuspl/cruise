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
package edu.snu.cay.dolphin.async.mlapps.serialization;

import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.common.math.linalg.VectorEntry;
import edu.snu.cay.common.math.linalg.VectorFactory;
import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;
import java.io.*;
import java.util.logging.Logger;

/**
 * Codec for sparse vector.
 */
public final class SparseVectorCodec implements Codec<Vector>, StreamingCodec<Vector> {
  private static final Logger LOG = Logger.getLogger(SparseVectorCodec.class.getName());
  private final VectorFactory vectorFactory;

  @Inject
  private SparseVectorCodec(final VectorFactory vectorFactory) {
    this.vectorFactory = vectorFactory;
  }

  @Override
  public byte[] encode(final Vector vector) {
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream(getNumBytes(vector));
         final DataOutputStream dos = new DataOutputStream(baos)) {
      encodeToStream(vector, dos);
      return baos.toByteArray();
    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }
  }

  @Override
  public void encodeToStream(final Vector vector, final DataOutputStream dos) {
    if (vector.isDense()) {
      LOG.warning("the given vector is not sparse.");
    }

    try {
      dos.writeInt(vector.length());
      dos.writeInt(vector.activeSize());
      for (final VectorEntry element : vector) {
        dos.writeInt(element.index());
        dos.writeDouble(element.value());
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
  public Vector decodeFromStream(final DataInputStream dis) {
    try {
      final int size = dis.readInt();
      final int numNonZeros = dis.readInt();
      final Vector vector = vectorFactory.createSparseZeros(size);
      for (int i = 0; i < numNonZeros; ++i) {
        final int index = dis.readInt();
        final double value = dis.readDouble();
        vector.set(index, value);
      }
      return vector;
    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }
  }

  public int getNumBytes(final Vector vector) {
    if (vector.isDense()) {
      LOG.warning("the given vector is not sparse.");
    }
    return 2 * Integer.SIZE + (Integer.SIZE + Double.SIZE) * vector.activeSize();
  }
}
