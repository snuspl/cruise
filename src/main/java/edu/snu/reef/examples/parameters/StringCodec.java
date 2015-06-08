package edu.snu.reef.examples.parameters;

import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;

public final class StringCodec implements Codec<String> {

  @Inject
  public StringCodec() {
  }

  public final byte[] encode(final String str) {
    return str.getBytes();
  }

  public final String decode(final byte[] data) {
    return new String(data);
  }
}
