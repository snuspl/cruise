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

import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.common.math.linalg.VectorFactory;
import edu.snu.cay.dolphin.examples.ml.data.LinearModel;
import edu.snu.cay.dolphin.examples.ml.data.LinearRegSummary;
import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;
import java.io.*;

public class LinearRegSummaryCodec implements Codec<LinearRegSummary> {
  private final VectorFactory vectorFactory;

  @Inject
  public LinearRegSummaryCodec(final VectorFactory vectorFactory) {
    this.vectorFactory = vectorFactory;
  }

  @Override
  public byte[] encode(final LinearRegSummary sgdSummary) {
    final LinearModel model = sgdSummary.getModel();
    final ByteArrayOutputStream baos = new ByteArrayOutputStream(Integer.SIZE // count
        + Double.SIZE // loss
        + Integer.SIZE // parameter size
        + Double.SIZE * model.getParameters().length());

    try (final DataOutputStream daos = new DataOutputStream(baos)) {
      daos.writeInt(sgdSummary.getCount());
      daos.writeDouble(sgdSummary.getLoss());
      daos.writeInt(model.getParameters().length());

      for (int i = 0; i < model.getParameters().length(); i++) {
        daos.writeDouble(model.getParameters().get(i));
      }

    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }

    return baos.toByteArray();
  }

  @Override
  public LinearRegSummary decode(final byte[] data) {
    final ByteArrayInputStream bais = new ByteArrayInputStream(data);
    final LinearModel model;
    final int count;
    final double loss;

    try (final DataInputStream dais = new DataInputStream(bais)) {
      count = dais.readInt();
      loss = dais.readDouble();
      final int vecSize = dais.readInt();
      final Vector v = vectorFactory.newDenseVector(vecSize);
      for (int i = 0; i < vecSize; i++) {
        v.set(i, dais.readDouble());
      }
      model = new LinearModel(v);
    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }
    return new LinearRegSummary(model, count, loss);
  }

}
