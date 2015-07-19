package edu.snu.cay.services.em.utils;

import edu.snu.cay.services.em.avro.AvroLongRange;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.lang.math.LongRange;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Utilities for AVRO.
 * Taken from org.apache.reef.io.network.naming.serialization.
 */
public final class AvroUtils {

  /**
   * Should not be instantiated
   */
  private AvroUtils() {
  }

  /**
   * Serializes the given avro object to a byte[].
   */
  public static <T> byte[] toBytes(final T avroObject, final Class<T> theClass) {
    final DatumWriter<T> datumWriter = new SpecificDatumWriter<>(theClass);
    final byte[] theBytes;
    try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      final BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
      datumWriter.write(avroObject, encoder);
      encoder.flush();
      out.flush();
      theBytes = out.toByteArray();
    } catch (final IOException e) {
      throw new RuntimeException("Unable to serialize an avro object", e);
    }
    return theBytes;
  }

  public static <T> T fromBytes(final byte[] theBytes, final Class<T> theClass) {
    final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(theBytes, null);
    final SpecificDatumReader<T> reader = new SpecificDatumReader<>(theClass);
    try {
      return reader.read(null, decoder);
    } catch (final IOException e) {
      throw new RuntimeException("Failed to deserialize an avro object", e);
    }
  }

  public static AvroLongRange convertLongRange(final LongRange longRange) {
    return AvroLongRange.newBuilder()
        .setMin(longRange.getMinimumLong())
        .setMax(longRange.getMaximumLong())
        .build();
  }
}
