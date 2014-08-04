package org.apache.reef.inmemory.common.replication;

import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Serialization utilities for Replication Rules using Avro.
 * The schema is defined in avro/replications.avsc
 */
public final class AvroReplicationSerializer {

  /**
   * Parse Rules from a JSON String
   */
  public static Rules fromString(String string) throws IOException {
    final DatumReader<Rules> reader = new SpecificDatumReader<>(Rules.class);
    final JsonDecoder decoder = DecoderFactory.get().jsonDecoder(Rules.getClassSchema(), string);
    return reader.read(null, decoder);
  }

  /**
   * Parse Rules from a JSON text InputStream
   */
  public static Rules fromStream(InputStream in) throws IOException {
    final DatumReader<Rules> reader = new SpecificDatumReader<>(Rules.class);
    final JsonDecoder decoder = DecoderFactory.get().jsonDecoder(Rules.getClassSchema(), in);
    return reader.read(null, decoder);
  }

  /**
   * Write Rules to an OutputStream as JSON text
   */
  public static void toStream(Rules rules, OutputStream out) throws IOException {
    final DatumWriter<Rules> rulesWriter = new SpecificDatumWriter<>(Rules.class);
    final JsonEncoder encoder = EncoderFactory.get().jsonEncoder(Rules.SCHEMA$, out);
    rulesWriter.write(rules, encoder);
    encoder.flush();
    out.flush();
  }
}
