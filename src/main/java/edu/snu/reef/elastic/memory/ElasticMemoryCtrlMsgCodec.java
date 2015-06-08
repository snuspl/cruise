package edu.snu.reef.elastic.memory;

import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;
import java.io.*;

public final class ElasticMemoryCtrlMsgCodec implements Codec<ElasticMemoryCtrlMsg> {

  @Inject
  public ElasticMemoryCtrlMsgCodec() {
  }

  @Override
  public byte[] encode(ElasticMemoryCtrlMsg msg) {
    try (final ByteArrayOutputStream bstream = new ByteArrayOutputStream()) {
      try (final DataOutputStream dstream = new DataOutputStream(bstream)) {
        dstream.writeUTF(msg.getDataClassName());
        dstream.writeUTF(msg.getDestId());
      }
      return bstream.toByteArray();

    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ElasticMemoryCtrlMsg decode(final byte[] data) {
    try (final ByteArrayInputStream bstream = new ByteArrayInputStream(data)) {
      try (final DataInputStream dstream = new DataInputStream(bstream)) {
        final String dataClassName = dstream.readUTF();
        final String destId = dstream.readUTF();
        return new ElasticMemoryCtrlMsg(dataClassName, destId);
      }
    } catch  (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
