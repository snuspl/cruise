package edu.snu.reef.elastic.memory;

import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;
import java.io.*;

public final class ElasticMemoryControlMessageCodec implements Codec<ElasticMemoryControlMessage> {

  @Inject
  public ElasticMemoryControlMessageCodec() {
  }

  @Override
  public byte[] encode(ElasticMemoryControlMessage msg) {
    try (final ByteArrayOutputStream bstream = new ByteArrayOutputStream()) {
      try (final DataOutputStream dstream = new DataOutputStream(bstream)) {
        dstream.writeUTF(msg.getString());
      }
      return bstream.toByteArray();

    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ElasticMemoryControlMessage decode(final byte[] data) {
    try (final ByteArrayInputStream bstream = new ByteArrayInputStream(data)) {
      try (final DataInputStream dstream = new DataInputStream(bstream)) {
        return new ElasticMemoryControlMessage(dstream.readUTF());
      }
    } catch  (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
