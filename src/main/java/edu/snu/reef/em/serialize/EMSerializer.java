package edu.snu.reef.em.serialize;

import edu.snu.reef.em.examples.parameters.StringCodec;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.io.serialization.SerializableCodec;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;

public class EMSerializer implements Serializer {

  private final Map<String, Codec> codecMap = new HashMap<>();
  private final Codec defaultCodec;

  @Inject
  private EMSerializer() {
    defaultCodec = new SerializableCodec();
    codecMap.put("String", new StringCodec());
  }

  @Override
  public Codec getCodec(final String name) {
    if (codecMap.containsKey(name)) {
      return codecMap.get(name);

    } else {
      return defaultCodec;
    }
  }
}
