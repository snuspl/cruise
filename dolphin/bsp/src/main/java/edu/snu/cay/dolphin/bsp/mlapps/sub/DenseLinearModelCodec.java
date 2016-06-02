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
package edu.snu.cay.dolphin.bsp.mlapps.sub;

import edu.snu.cay.dolphin.bsp.mlapps.data.LinearModel;
import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;
import java.io.*;

public final class DenseLinearModelCodec implements Codec<LinearModel>, StreamingCodec<LinearModel> {

  private final DenseVectorCodec denseVectorCodec;

  @Inject
  private DenseLinearModelCodec(final DenseVectorCodec denseVectorCodec) {
    this.denseVectorCodec = denseVectorCodec;
  }

  @Override
  public byte[] encode(final LinearModel model) {
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream(getNumBytes(model));
         final DataOutputStream dos = new DataOutputStream(baos)) {
      encodeToStream(model, dos);
      return baos.toByteArray();
    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }
  }

  @Override
  public void encodeToStream(final LinearModel model, final DataOutputStream dataOutputStream) {
    denseVectorCodec.encodeToStream(model.getParameters(), dataOutputStream);
  }

  @Override
  public LinearModel decode(final byte[] bytes) {
    try (final DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes))) {
      return decodeFromStream(dis);
    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }
  }

  @Override
  public LinearModel decodeFromStream(final DataInputStream dataInputStream) {
    return new LinearModel(denseVectorCodec.decodeFromStream(dataInputStream));
  }

  public int getNumBytes(final LinearModel model) {
    return denseVectorCodec.getNumBytes(model.getParameters());
  }
}
