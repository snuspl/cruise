package edu.snu.reef.em.serializer;

import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.DefaultImplementation;

@DefaultImplementation(EMSerializer.class)
public interface Serializer {

  Codec getCodec(String name);
}
