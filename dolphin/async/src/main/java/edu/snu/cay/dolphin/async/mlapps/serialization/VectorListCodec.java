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
package edu.snu.cay.dolphin.async.mlapps.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;
import javax.inject.Inject;

import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.common.math.linalg.VectorFactory;
import org.apache.reef.io.serialization.Codec;

/**
 * Codec for vector list.
 */
public final class VectorListCodec implements Codec<List<Vector>> {

  private final VectorFactory vectorFactory;

  @Inject
  private VectorListCodec(final VectorFactory vectorFactory) {
    this.vectorFactory = vectorFactory;
  }

  public byte[] encode(final List<Vector> list) {

    // This codec assume that vectors have the same length
    int vectorLength = 0;
    for (final Vector vector : list) {
      vectorLength = vector.length();
    }

    // (1) the size of the list, (2) the length of each vector, and (3) the sum of all vectors' lengths in total.
    try (ByteArrayOutputStream baos =
             new ByteArrayOutputStream(Integer.SIZE + Integer.SIZE + Float.SIZE * vectorLength * list.size())) {
      try (DataOutputStream dos = new DataOutputStream(baos)) {
        dos.writeInt(list.size());
        dos.writeInt(vectorLength);

        for (final Vector vector : list) {
          for (int i = 0; i < vectorLength; ++i) {
            dos.writeFloat(vector.get(i));
          }
        }
      }

      return baos.toByteArray();
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  public List<Vector> decode(final byte[] data) {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(data)) {
      final List<Vector> resultList = new LinkedList<>();

      try (DataInputStream dais = new DataInputStream(bais)) {
        final int listSize = dais.readInt();
        final int vectorLength = dais.readInt();
        for (int i = 0; i < listSize; ++i) {
          final Vector vector = vectorFactory.createDenseZeros(vectorLength);
          for (int j = 0; j < vectorLength; ++j) {
            vector.set(j, dais.readFloat());
          }
          resultList.add(vector);
        }
      }

      return resultList;
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
