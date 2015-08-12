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
import edu.snu.cay.dolphin.examples.ml.data.LogisticRegSummary;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;
import java.io.*;

public class LogisticRegSummaryCodec implements Codec<LogisticRegSummary> {

  @Inject
  public LogisticRegSummaryCodec() {
  }

  @Override
  public byte[] encode(final LogisticRegSummary summary) {
    final LinearModel model = summary.getModel();
    final ByteArrayOutputStream baos = new ByteArrayOutputStream(Integer.SIZE //count
        + Integer.SIZE // posNum
        + Integer.SIZE // negNum
        + Integer.SIZE // parameter size
        + Double.SIZE * model.getParameters().size());

    try (final DataOutputStream daos = new DataOutputStream(baos)) {
      daos.writeInt(summary.getCount());
      daos.writeInt(summary.getPosNum());
      daos.writeInt(summary.getNegNum());
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
  public LogisticRegSummary decode(final byte[] data) {
    final ByteArrayInputStream bais = new ByteArrayInputStream(data);
    final LinearModel model;
    final int count;
    final int posNum;
    final int negNum;

    try (final DataInputStream dais = new DataInputStream(bais)) {
      count = dais.readInt();
      posNum = dais.readInt();
      negNum = dais.readInt();
      final int vecSize = dais.readInt();
      final Vector v = new DenseVector(vecSize);
      for (int i = 0; i < vecSize; i++) {
        v.set(i, dais.readDouble());
      }
      model = new LinearModel(v);

    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }

    return new LogisticRegSummary(model, count, posNum, negNum);
  }
}
