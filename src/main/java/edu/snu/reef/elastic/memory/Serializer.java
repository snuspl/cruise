package edu.snu.reef.elastic.memory;

import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.DefaultImplementation;

@DefaultImplementation(EMSerializer.class)
public interface Serializer {

  public Codec getCodec(String name);
}
