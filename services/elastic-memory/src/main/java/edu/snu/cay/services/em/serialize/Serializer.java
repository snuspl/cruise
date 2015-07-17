package edu.snu.cay.services.em.serialize;

import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.DefaultImplementation;

@DefaultImplementation(JavaSerializer.class)
public interface Serializer {

  Codec getCodec(String name);
}
