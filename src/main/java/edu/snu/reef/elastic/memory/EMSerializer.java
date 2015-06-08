package edu.snu.reef.elastic.memory;

import edu.snu.reef.examples.parameters.StringCodec;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.io.serialization.SerializableCodec;

import javax.inject.Inject;

public class EMSerializer implements Serializer {

  @Inject
  public EMSerializer() {
  }

  @Override
  public Codec getCodec(final String name) {
    switch (name) {
      case "String":
        return new StringCodec();

      default:
        return new SerializableCodec();
    }
  }
}
