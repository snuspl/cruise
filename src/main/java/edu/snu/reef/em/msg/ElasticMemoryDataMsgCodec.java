package edu.snu.reef.em.msg;

import org.apache.reef.io.network.impl.StreamingCodec;

import javax.inject.Inject;
import java.io.*;

public final class ElasticMemoryDataMsgCodec implements StreamingCodec<ElasticMemoryDataMsg> {

  @Inject
  public ElasticMemoryDataMsgCodec() {
  }

  @Override
  public byte[] encode(final ElasticMemoryDataMsg msg) {
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
  public void encodeToStream(final ElasticMemoryDataMsg msg, final DataOutputStream dstream) {
    try {
      final byte[][] dataArray = msg.getData();
      dstream.writeUTF(msg.getFrom());
      dstream.writeUTF(msg.getTo());
      dstream.writeUTF(msg.getDataClassName());
      dstream.writeInt(dataArray.length);
      for (final byte[] data : dataArray) {
        dstream.writeInt(data.length);
        dstream.write(data);
      }

    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ElasticMemoryDataMsg decode(final byte[] data) {
    try (final ByteArrayInputStream bstream = new ByteArrayInputStream(data)) {
      try (final DataInputStream dstream = new DataInputStream(bstream)) {
        return decodeFromStream(dstream);
      }
    } catch  (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ElasticMemoryDataMsg decodeFromStream(final DataInputStream dstream) {
    try {
      final String from = dstream.readUTF();
      final String to = dstream.readUTF();
      final String dataClassName = dstream.readUTF();
      final byte[][] data = new byte[dstream.readInt()][];
      for (int index = 0; index < data.length; index++) {
        data[index] = new byte[dstream.readInt()];
        dstream.readFully(data[index]);
      }

      return new ElasticMemoryDataMsg(from, to, dataClassName, data);

    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
