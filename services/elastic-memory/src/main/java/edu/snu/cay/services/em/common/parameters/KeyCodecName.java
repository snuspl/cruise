package edu.snu.cay.services.em.common.parameters;

import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "codec class for encoding and decoding key objects in EM")
public class KeyCodecName implements Name<Codec> {
}
