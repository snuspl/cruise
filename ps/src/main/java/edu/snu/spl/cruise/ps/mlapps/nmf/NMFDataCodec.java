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
package edu.snu.spl.cruise.ps.mlapps.nmf;

import edu.snu.spl.cruise.common.math.linalg.Vector;
import edu.snu.spl.cruise.ps.mlapps.serialization.DenseVectorCodec;
import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

final class NMFDataCodec implements Codec<NMFData>, StreamingCodec<NMFData> {
  private final DenseVectorCodec denseVectorCodec;

  @Inject
  private NMFDataCodec(final DenseVectorCodec denseVectorCodec) {
    this.denseVectorCodec = denseVectorCodec;
  }

  @Override
  public byte[] encode(final NMFData nmfData) {
    final int numBytes =
        denseVectorCodec.getNumBytes(nmfData.getVector()) + getNumBytes(nmfData.getColumns()) + Integer.BYTES;
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream(numBytes);
         DataOutputStream daos = new DataOutputStream(baos)) {
      encodeToStream(nmfData, daos);
      return baos.toByteArray();
    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }
  }

  @Override
  public NMFData decode(final byte[] bytes) {
    try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes))) {
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
      final List<Pair<Integer, Float>> columns = decodeColumns(dais);
      final Vector vector = denseVectorCodec.decodeFromStream(dais);
      return new NMFData(rowIndex, columns, vector);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Computes the number of bytes of columns for allocating buffer. Note that an extra integer is written
   * to record the number of the columns {@link #encodeColumns(List, DataOutputStream)}.
   * @return the total number of bytes of the encoded columns
   */
  private int getNumBytes(final List<Pair<Integer, Float>> columns) {
    return Integer.BYTES + columns.size() * (Integer.BYTES + Float.BYTES);
  }

  private void encodeColumns(final List<Pair<Integer, Float>> columns,
                             final DataOutputStream daos) throws IOException {
    daos.writeInt(columns.size());
    for (final Pair<Integer, Float> column : columns) {
      daos.writeInt(column.getFirst());
      daos.writeFloat(column.getSecond());
    }
  }

  private List<Pair<Integer, Float>> decodeColumns(final DataInputStream dais)
      throws IOException {
    final int size = dais.readInt();
    final List<Pair<Integer, Float>> columns = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      final int first = dais.readInt();
      final float second = dais.readFloat();
      columns.add(new Pair<>(first, second));
    }
    return columns;
  }
}
