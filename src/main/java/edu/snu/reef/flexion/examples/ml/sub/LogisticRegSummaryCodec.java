package edu.snu.reef.flexion.examples.ml.sub;


import edu.snu.reef.flexion.examples.ml.data.LinearModel;
import edu.snu.reef.flexion.examples.ml.data.LogisticRegSummary;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;
import java.io.*;

public class LogisticRegSummaryCodec implements Codec<LogisticRegSummary> {

  @Inject
  public LogisticRegSummaryCodec(){

  }

  @Override
  public byte[] encode(LogisticRegSummary summary) {
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

    LinearModel model;
    int count;
    int posNum;
    int negNum;
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
