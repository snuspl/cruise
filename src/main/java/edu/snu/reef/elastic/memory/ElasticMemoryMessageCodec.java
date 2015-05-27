package edu.snu.reef.elastic.memory;

import org.apache.reef.io.network.impl.StreamingCodec;

import javax.inject.Inject;
import java.io.*;

public final class ElasticMemoryMessageCodec implements StreamingCodec<ElasticMemoryMessage> {

  @Inject
  public ElasticMemoryMessageCodec() {
  }

  @Override
  public byte[] encode(final ElasticMemoryMessage msg) {
    try (final ByteArrayOutputStream bstream = new ByteArrayOutputStream()) {
      try (final DataOutputStream dstream = new DataOutputStream(bstream)) {
        encodeToStream(msg, dstream);
      }
      return bstream.toByteArray();

    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void encodeToStream(final ElasticMemoryMessage msg, final DataOutputStream dstream) {
    try {
      dstream.writeUTF(msg.getFrom());
      dstream.writeUTF(msg.getTo());
      dstream.writeInt(ElasticMemoryMessage.Type.toInt(msg.getMsgType()));
      dstream.writeUTF(msg.getDataClassName());
      dstream.writeInt(msg.getData().length);
      dstream.write(msg.getData());

    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ElasticMemoryMessage decode(final byte[] data) {
    try (final ByteArrayInputStream bstream = new ByteArrayInputStream(data)) {
      try (final DataInputStream dstream = new DataInputStream(bstream)) {
        return decodeFromStream(dstream);
      }
    } catch  (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ElasticMemoryMessage decodeFromStream(final DataInputStream dstream) {
    try {
      final String from = dstream.readUTF();
      final String to = dstream.readUTF();
      final ElasticMemoryMessage.Type msgType = ElasticMemoryMessage.Type.toType(dstream.readInt());
      final String dataClassName = dstream.readUTF();
      final int dataLength  = dstream.readInt();
      final byte[] data = new byte[dataLength];
      dstream.readFully(data);

      return new ElasticMemoryMessage(from, to, msgType, dataClassName, data);

    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
