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
package edu.snu.cay.dolphin.examples.matmul;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

public final class IndexedVectorListCodec implements Codec<List<IndexedVector>> {

  @Inject
  private IndexedVectorListCodec() {
  }

  @Override
  public byte[] encode(final List<IndexedVector> list) {
    int totalSizeOfIndexedVector = 0;
    for (final IndexedVector indexedVector : list) {
      totalSizeOfIndexedVector += Integer.SIZE + Integer.SIZE + indexedVector.getVector().size() * Double.SIZE;
    }

    final ByteArrayOutputStream baos = new ByteArrayOutputStream(Integer.SIZE + totalSizeOfIndexedVector);

    try (final DataOutputStream daos = new DataOutputStream(baos)) {
      daos.writeInt(list.size());
      for (final IndexedVector indexedVector : list) {
        daos.writeInt(indexedVector.getIndex());
        final Vector vector = indexedVector.getVector();
        final int vectorSize = vector.size();
        daos.writeInt(vectorSize);
        for (int i = 0; i < vectorSize; i++) {
          daos.writeDouble(vector.get(i));
        }
      }
    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }

    return baos.toByteArray();
  }

  @Override
  public List<IndexedVector> decode(final byte[] bytes) {
    final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    try (final DataInputStream dais = new DataInputStream(bais)) {
      final int size = dais.readInt();
      final List<IndexedVector> resultList = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        final int index = dais.readInt();
        final int vectorSize = dais.readInt();
        final Vector vector = new DenseVector(vectorSize);
        for (int j = 0; j < vectorSize; j++) {
          vector.set(j, dais.readDouble());
        }

        resultList.add(new IndexedVector(index, vector));
      }

      return resultList;
    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }
  }
}
