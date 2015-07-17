package edu.snu.reef.em.serialize;

import org.apache.reef.io.serialization.Codec;
import org.apache.reef.io.serialization.SerializableCodec;

import javax.inject.Inject;

public class JavaSerializer implements Serializer {

  private final Codec defaultCodec;

  @Inject
  private JavaSerializer() {
    defaultCodec = new SerializableCodec();
  }

  @Override
  public Codec getCodec(final String name) {
    return defaultCodec;
  }
}
