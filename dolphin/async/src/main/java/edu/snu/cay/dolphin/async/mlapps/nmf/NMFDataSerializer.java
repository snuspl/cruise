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
import java.util.logging.Logger;

/**
 * Serializer that provides codec for (de-)serializing data used in NMF.
 */
final class NMFDataSerializer implements Serializer {
  private static final Logger LOG = Logger.getLogger(NMFDataSerializer.class.getName());
  private final DenseVectorCodec denseVectorCodec;

  @Inject
  private NMFDataSerializer(final DenseVectorCodec denseVectorCodec) {
    this.denseVectorCodec = denseVectorCodec;
  }

  @Override
  public Codec getCodec() {
    return new NMFDataCodec();
  }

  private final class NMFDataCodec implements Codec<NMFData>, StreamingCodec<NMFData> {
    @Override
    public byte[] encode(final NMFData nmfData) {
      final int numBytes = getNumBytes(nmfData.getVector()) + getNumBytes(nmfData.getColumns()) + Integer.SIZE;
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

  private int getNumBytes(final Vector vector) {
    if (!vector.isDense()) {
      LOG.warning("the given vector is not dense.");
    }
    return Integer.SIZE + Double.SIZE * vector.length();
  }

  private int getNumBytes(final List<Pair<Integer, Double>> columns) {
    return columns.size() * (Integer.SIZE + Double.SIZE);
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
