package edu.snu.reef.examples.elastic.migration;

import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;
import java.io.*;

public final class ReadyCodec implements Codec<Boolean> {

  @Inject
  public ReadyCodec() {
  }

  @Override
  public byte[] encode(final Boolean bool) {
    try (final ByteArrayOutputStream bstream = new ByteArrayOutputStream()) {
      try (final DataOutputStream dstream = new DataOutputStream(bstream)) {
        dstream.writeBoolean(bool);
      }
      return bstream.toByteArray();

    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Boolean decode(final byte[] data) {
    try (final ByteArrayInputStream bstream = new ByteArrayInputStream(data)) {
      try (final DataInputStream dstream = new DataInputStream(bstream)) {
        return dstream.readBoolean();
      }
    } catch  (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
