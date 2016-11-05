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
import edu.snu.cay.dolphin.bsp.mlapps.data.LogisticRegSummary;
import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;
import java.io.*;

/**
 * Codec for a summary of logistic regression with a linear model that uses a dense vector as a model parameter.
 * Internally uses {@link DenseLinearModelCodec}.
 */
public final class DenseLogisticRegSummaryCodec implements Codec<LogisticRegSummary> {

  private final DenseLinearModelCodec denseLinearModelCodec;

  @Inject
  private DenseLogisticRegSummaryCodec(final DenseLinearModelCodec denseLinearModelCodec) {
    this.denseLinearModelCodec = denseLinearModelCodec;
  }

  @Override
  public byte[] encode(final LogisticRegSummary summary) {
    final LinearModel model = summary.getModel();
    final ByteArrayOutputStream baos = new ByteArrayOutputStream(Integer.SIZE //count
        + Integer.SIZE // posNum
        + Integer.SIZE // negNum
        + Integer.SIZE // parameter size
        + denseLinearModelCodec.getNumBytes(model));

    try (DataOutputStream daos = new DataOutputStream(baos)) {
      daos.writeInt(summary.getCount());
      daos.writeInt(summary.getPosNum());
      daos.writeInt(summary.getNegNum());
      denseLinearModelCodec.encodeToStream(model, daos);
    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }

    return baos.toByteArray();
  }


  @Override
  public LogisticRegSummary decode(final byte[] data) {
    try (DataInputStream dais = new DataInputStream(new ByteArrayInputStream(data))) {
      final int count = dais.readInt();
      final int posNum = dais.readInt();
      final int negNum = dais.readInt();
      final LinearModel model = denseLinearModelCodec.decodeFromStream(dais);
      return new LogisticRegSummary(model, count, posNum, negNum);
    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }
  }
}
