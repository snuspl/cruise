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

import edu.snu.cay.dolphin.examples.ml.data.Row;
import org.apache.mahout.math.Vector;
import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Codec for row with a dense vector.
 */
public final class DenseRowCodec implements Codec<Row>, StreamingCodec<Row> {

  private final DenseVectorCodec denseVectorCodec;

  @Inject
  private DenseRowCodec(final DenseVectorCodec denseVectorCodec) {
    this.denseVectorCodec = denseVectorCodec;
  }

  @Override
  public byte[] encode(final Row row) {
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream(getNumBytes(row));
         final DataOutputStream daos = new DataOutputStream(baos)) {
      encodeToStream(row, daos);
      return baos.toByteArray();
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void encodeToStream(final Row row, final DataOutputStream daos) {
    try {
      daos.writeDouble(row.getOutput());
      denseVectorCodec.encodeToStream(row.getFeature(), daos);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Row decode(final byte[] bytes) {
    try (final DataInputStream dais = new DataInputStream(new ByteArrayInputStream(bytes))) {
      return decodeFromStream(dais);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Row decodeFromStream(final DataInputStream dais) {
    try {
      final double output = dais.readDouble();
      final Vector feature = denseVectorCodec.decodeFromStream(dais);
      return new Row(output, feature);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  public int getNumBytes(final Row row) {
    return Double.SIZE + denseVectorCodec.getNumBytes(row.getFeature());
  }
}
