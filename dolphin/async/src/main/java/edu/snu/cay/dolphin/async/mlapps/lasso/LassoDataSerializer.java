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
package edu.snu.cay.dolphin.async.mlapps.lasso;

import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.dolphin.async.mlapps.serialization.DenseVectorCodec;
import edu.snu.cay.services.em.serialize.Serializer;
import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;
import java.io.*;

/**
 * Serializer that provides codec for (de-)serializing data used in MLR.
 */
final class LassoDataSerializer implements Serializer {
  private final DenseVectorCodec denseVectorCodec;
  private final LassoDataSGDCodec lassoDataSGDCodec = new LassoDataSGDCodec();

  @Inject
  private LassoDataSerializer(final DenseVectorCodec denseVectorCodec) {
    this.denseVectorCodec = denseVectorCodec;
  }

  @Override
  public Codec getCodec() {
    return lassoDataSGDCodec;
  }

  private final class LassoDataSGDCodec implements Codec<LassoDataSGD>, StreamingCodec<LassoDataSGD> {
    @Override
    public byte[] encode(final LassoDataSGD lassoDataSGD) {
      final int numBytes = denseVectorCodec.getNumBytes(lassoDataSGD.getFeature()) + Integer.BYTES;
      try (ByteArrayOutputStream baos = new ByteArrayOutputStream(numBytes);
           DataOutputStream daos = new DataOutputStream(baos)) {
        encodeToStream(lassoDataSGD, daos);
        return baos.toByteArray();
      } catch (final IOException e) {
        throw new RuntimeException(e.getCause());
      }
    }

    @Override
    public LassoDataSGD decode(final byte[] bytes) {
      try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes))) {
        return decodeFromStream(dis);
      } catch (final IOException e) {
        throw new RuntimeException(e.getCause());
      }
    }

    @Override
    public void encodeToStream(final LassoDataSGD lassoDataSGD, final DataOutputStream daos) {
      try {
        denseVectorCodec.encodeToStream(lassoDataSGD.getFeature(), daos);
        daos.writeDouble(lassoDataSGD.getValue());
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public LassoDataSGD decodeFromStream(final DataInputStream dais) {
      try {
        final Vector featureVector = denseVectorCodec.decodeFromStream(dais);
        final double value = dais.readDouble();
        return new LassoDataSGD(featureVector, value);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
