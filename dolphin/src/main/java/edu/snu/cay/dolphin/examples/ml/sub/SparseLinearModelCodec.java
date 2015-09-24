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

import edu.snu.cay.dolphin.examples.ml.data.LinearModel;
import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

public final class SparseLinearModelCodec implements Codec<LinearModel>, StreamingCodec<LinearModel> {

  private final SparseVectorCodec sparseVectorCodec;

  @Inject
  private SparseLinearModelCodec(final SparseVectorCodec sparseVectorCodec) {
    this.sparseVectorCodec = sparseVectorCodec;
  }

  @Override
  public byte[] encode(final LinearModel model) {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream(getNumBytes(model));
    final DataOutputStream dos = new DataOutputStream(baos);
    encodeToStream(model, dos);
    return baos.toByteArray();
  }

  @Override
  public void encodeToStream(final LinearModel model, final DataOutputStream dataOutputStream) {
    sparseVectorCodec.encodeToStream(model.getParameters(), dataOutputStream);
  }

  @Override
  public LinearModel decode(final byte[] bytes) {
    final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    final DataInputStream dis = new DataInputStream(bais);
    return decodeFromStream(dis);
  }

  @Override
  public LinearModel decodeFromStream(final DataInputStream dataInputStream) {
    return new LinearModel(sparseVectorCodec.decodeFromStream(dataInputStream));
  }

  public int getNumBytes(final LinearModel model) {
    return sparseVectorCodec.getNumBytes(model.getParameters());
  }
}
