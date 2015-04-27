/**
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
package edu.snu.reef.flexion.examples.ml.sub;

import edu.snu.reef.flexion.examples.ml.data.LinearModel;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;
import java.io.*;

public class LinearModelCodec implements Codec<LinearModel> {

  @Inject
  public LinearModelCodec() {
  }

  @Override
  public byte[] encode(LinearModel model) {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream(Integer.SIZE +
        Double.SIZE * model.getParameters().size());
    try (final DataOutputStream daos = new DataOutputStream(baos)) {
      daos.writeInt(model.getParameters().size());

      for (int i = 0; i < model.getParameters().size(); i++) {
        daos.writeDouble(model.getParameters().get(i));
      }

    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }

    return baos.toByteArray();
  }


  @Override
  public LinearModel decode(final byte[] data) {
    final ByteArrayInputStream bais = new ByteArrayInputStream(data);

    LinearModel model;
    try (final DataInputStream dais = new DataInputStream(bais)) {
      final int vecSize = dais.readInt();
      final Vector v = new DenseVector(vecSize);
      for (int i = 0; i < vecSize; i++) {
        v.set(i, dais.readDouble());
      }
      model = new LinearModel(v);

    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }

    return model;
  }
}
