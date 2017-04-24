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
package edu.snu.cay.dolphin.async.mlapps.lasso;

import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.dolphin.async.mlapps.serialization.SparseVectorCodec;
import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;
import java.io.*;

/**
 * A codec for (de-)serializing data used in Lasso.
 */
public final class LassoDataCodec implements Codec<LassoData>, StreamingCodec<LassoData> {
  private final SparseVectorCodec sparseVectorCodec;

  @Inject
  LassoDataCodec(final SparseVectorCodec sparseVectorCodec) {
    this.sparseVectorCodec = sparseVectorCodec;
  }

  @Override
  public byte[] encode(final LassoData lassoData) {
    final int numBytes = sparseVectorCodec.getNumBytes(lassoData.getFeature()) + Integer.BYTES;
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream(numBytes);
         DataOutputStream daos = new DataOutputStream(baos)) {
      encodeToStream(lassoData, daos);
      return baos.toByteArray();
    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }
  }

  @Override
  public LassoData decode(final byte[] bytes) {
    try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes))) {
      return decodeFromStream(dis);
    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }
  }

  @Override
  public void encodeToStream(final LassoData lassoData, final DataOutputStream daos) {
    try {
      sparseVectorCodec.encodeToStream(lassoData.getFeature(), daos);
      daos.writeDouble(lassoData.getValue());
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public LassoData decodeFromStream(final DataInputStream dais) {
    try {
      final Vector featureVector = sparseVectorCodec.decodeFromStream(dais);
      final double value = dais.readDouble();
      return new LassoData(featureVector, value);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
