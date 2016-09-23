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
package edu.snu.cay.dolphin.async.mlapps.nmf;

import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.dolphin.async.mlapps.serialization.DenseVectorCodec;
import edu.snu.cay.services.em.serialize.Serializer;
import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Serializer that provides codec for (de-)serializing data used in NMF.
 */
final class NMFDataSerializer implements Serializer {
  private static final int INTEGER_BYTES = 4; // size of integer in bytes
  private static final int DOUBLE_BYTES = 8; // size of double in bytes

  private final DenseVectorCodec denseVectorCodec;
  private final NMFDataCodec nmfDataCodec = new NMFDataCodec();

  @Inject
  private NMFDataSerializer(final DenseVectorCodec denseVectorCodec) {
    this.denseVectorCodec = denseVectorCodec;
  }

  @Override
  public Codec getCodec() {
    return nmfDataCodec;
  }

  private final class NMFDataCodec implements Codec<NMFData>, StreamingCodec<NMFData> {
    @Override
    public byte[] encode(final NMFData nmfData) {
      final int numBytes =
          denseVectorCodec.getNumBytes(nmfData.getVector()) + getNumBytes(nmfData.getColumns()) + INTEGER_BYTES;
      try (final ByteArrayOutputStream baos = new ByteArrayOutputStream(numBytes);
           final DataOutputStream daos = new DataOutputStream(baos)) {
        encodeToStream(nmfData, daos);
        return baos.toByteArray();
      } catch (final IOException e) {
        throw new RuntimeException(e.getCause());
      }
    }

    @Override
    public NMFData decode(final byte[] bytes) {
      try (final DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes))) {
        return decodeFromStream(dis);
      } catch (final IOException e) {
        throw new RuntimeException(e.getCause());
      }
    }

    @Override
    public void encodeToStream(final NMFData nmfData, final DataOutputStream daos) {
      try {
        daos.writeInt(nmfData.getRowIndex());
        encodeColumns(nmfData.getColumns(), daos);
        denseVectorCodec.encodeToStream(nmfData.getVector(), daos);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public NMFData decodeFromStream(final DataInputStream dais) {
      try {
        final int rowIndex = dais.readInt();
        final List<Pair<Integer, Double>> columns = decodeColumns(dais);
        final Vector vector = denseVectorCodec.decodeFromStream(dais);
        return new NMFData(rowIndex, columns, vector);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Computes the number of bytes of columns for allocating buffer. Note that an extra integer is written
   * to record the number of the columns {@link #encodeColumns(List, DataOutputStream)}.
   * @return the total number of bytes of the encoded columns
   */
  private int getNumBytes(final List<Pair<Integer, Double>> columns) {
    return INTEGER_BYTES + columns.size() * (INTEGER_BYTES + DOUBLE_BYTES);
  }

  private void encodeColumns(final List<Pair<Integer, Double>> columns,
                             final DataOutputStream daos) throws IOException {
    daos.writeInt(columns.size());
    for (final Pair<Integer, Double> column : columns) {
      daos.writeInt(column.getFirst());
      daos.writeDouble(column.getSecond());
    }
  }

  private List<Pair<Integer, Double>> decodeColumns(final DataInputStream dais)
      throws IOException {
    final int size = dais.readInt();
    final List<Pair<Integer, Double>> columns = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      final int first = dais.readInt();
      final double second = dais.readDouble();
      columns.add(new Pair<>(first, second));
    }
    return columns;
  }
}
