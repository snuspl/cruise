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
