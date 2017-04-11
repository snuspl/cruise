package edu.snu.cay.dolphin.async.metric;

import edu.snu.cay.dolphin.async.metric.avro.DolphinMetrics;
import edu.snu.cay.utils.AvroUtils;
import org.apache.reef.io.network.impl.StreamingCodec;

import javax.inject.Inject;
import java.io.*;

/**
 * Codec that (de-)serializes the Dolphin-specific metrics.
 */
public class ETDolphinMetricCodec implements StreamingCodec<DolphinMetrics> {
  @Inject
  private ETDolphinMetricCodec() {
  }

  @Override
  public void encodeToStream(final DolphinMetrics dolphinMetrics, final DataOutputStream dos) {
    try {
      final byte[] encoded = AvroUtils.toBytes(dolphinMetrics, DolphinMetrics.class);
      dos.writeInt(encoded.length);
      dos.write(encoded);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public DolphinMetrics decodeFromStream(final DataInputStream dis) {
    try {
      final int length = dis.readInt();
      final byte[] buffer = new byte[length];
      dis.readFully(buffer);
      return decode(buffer);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public DolphinMetrics decode(final byte[] bytes) {
    return AvroUtils.fromBytes(bytes, DolphinMetrics.class);
  }

  @Override
  public byte[] encode(final DolphinMetrics dolphinMetrics) {
    return AvroUtils.toBytes(dolphinMetrics, DolphinMetrics.class);
  }
}
