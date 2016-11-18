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
package edu.snu.cay.dolphin.async.mlapps.mlr;

import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.dolphin.async.mlapps.serialization.SparseVectorCodec;
import edu.snu.cay.services.em.serialize.Serializer;
import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;
import java.io.*;

/**
 * Serializer that provides codec for (de-)serializing data used in MLR.
 */
final class MLRDataSerializer implements Serializer {
  private final SparseVectorCodec sparseVectorCodec;
  private final MLRDataCodec mlrDataCodec = new MLRDataCodec();

  @Inject
  private MLRDataSerializer(final SparseVectorCodec sparseVectorCodec) {
    this.sparseVectorCodec = sparseVectorCodec;
  }

  @Override
  public Codec getCodec() {
    return mlrDataCodec;
  }

  private final class MLRDataCodec implements Codec<MLRData>, StreamingCodec<MLRData> {
    @Override
    public byte[] encode(final MLRData mlrData) {
      final int numBytes = sparseVectorCodec.getNumBytes(mlrData.getFeature()) + Integer.BYTES;
      try (ByteArrayOutputStream baos = new ByteArrayOutputStream(numBytes);
           DataOutputStream daos = new DataOutputStream(baos)) {
        encodeToStream(mlrData, daos);
        return baos.toByteArray();
      } catch (final IOException e) {
        throw new RuntimeException(e.getCause());
      }
    }

    @Override
    public MLRData decode(final byte[] bytes) {
      try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes))) {
        return decodeFromStream(dis);
      } catch (final IOException e) {
        throw new RuntimeException(e.getCause());
      }
    }

    @Override
    public void encodeToStream(final MLRData mlrData, final DataOutputStream daos) {
      try {
        sparseVectorCodec.encodeToStream(mlrData.getFeature(), daos);
        daos.writeInt(mlrData.getOutput());
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public MLRData decodeFromStream(final DataInputStream dais) {
      try {
        final Vector featureVector = sparseVectorCodec.decodeFromStream(dais);
        final int output = dais.readInt();
        return new MLRData(featureVector, output);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
