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
package edu.snu.cay.dolphin.async.dnn.data;

import edu.snu.cay.dolphin.async.dnn.blas.Matrix;
import edu.snu.cay.dolphin.async.dnn.layers.LayerParameter;
import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;
import java.io.*;

/**
 * Serialization codec for a layer parameter (weights and biases).
 * Internally uses {@link MatrixCodec}.
 */
public final class LayerParameterCodec implements StreamingCodec<LayerParameter>, Codec<LayerParameter> {

  private final MatrixCodec matrixCodec;

  @Inject
  private LayerParameterCodec(final MatrixCodec matrixCodec) {
    this.matrixCodec = matrixCodec;
  }

  @Override
  public byte[] encode(final LayerParameter layerParameter) {
    try (ByteArrayOutputStream bstream = new ByteArrayOutputStream();
         DataOutputStream dstream = new DataOutputStream(bstream)) {
      encodeToStream(layerParameter, dstream);
      return bstream.toByteArray();

    } catch (final IOException e) {
      throw new RuntimeException("IOException during LayerParameterArrayCodec.encode()", e);
    }
  }

  @Override
  public void encodeToStream(final LayerParameter layerParameter, final DataOutputStream dstream) {
    matrixCodec.encodeToStream(layerParameter.getWeightParam(), dstream);
    matrixCodec.encodeToStream(layerParameter.getBiasParam(), dstream);
  }

  @Override
  public LayerParameter decode(final byte[] data) {
    try (DataInputStream dstream = new DataInputStream(new ByteArrayInputStream(data))) {
      return decodeFromStream(dstream);

    } catch (final IOException e) {
      throw new RuntimeException("IOException during LayerParameterArrayCodec.decode()", e);
    }
  }

  @Override
  public LayerParameter decodeFromStream(final DataInputStream dstream) {
    final Matrix weightParam = matrixCodec.decodeFromStream(dstream);
    final Matrix biasParam = matrixCodec.decodeFromStream(dstream);
    return new LayerParameter(weightParam, biasParam);
  }
}
