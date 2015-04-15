package edu.snu.reef.flexion.examples.ml.sub;


import edu.snu.reef.flexion.examples.ml.data.LinearModel;
import edu.snu.reef.flexion.examples.ml.data.LinearRegSummary;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;
import java.io.*;

public class LinearRegSummaryCodec implements Codec<LinearRegSummary> {

  @Inject
  public LinearRegSummaryCodec(){

  }

  @Override
  public byte[] encode(LinearRegSummary sgdSummary) {
    final LinearModel model = sgdSummary.getModel();
    final ByteArrayOutputStream baos = new ByteArrayOutputStream(Integer.SIZE // count
        + Double.SIZE // loss
        + Integer.SIZE // parameter size
        + Double.SIZE * model.getParameters().size());
    try (final DataOutputStream daos = new DataOutputStream(baos)) {
      daos.writeInt(sgdSummary.getCount());
      daos.writeDouble(sgdSummary.getLoss());
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
  public LinearRegSummary decode(final byte[] data) {
    final ByteArrayInputStream bais = new ByteArrayInputStream(data);

    LinearModel model;
    int count;
    double loss;
    try (final DataInputStream dais = new DataInputStream(bais)) {
      count = dais.readInt();
      loss = dais.readDouble();
      final int vecSize = dais.readInt();
      final Vector v = new DenseVector(vecSize);
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
