/*
 * Copyright (C) 2015 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.cay.utils;

import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Utilities for AVRO.
 * Taken from org.apache.reef.io.network.naming.serialization.
 */
public final class AvroUtils {

  /**
   * Should not be instantiated.
   */
  private AvroUtils() {
  }

  /**
   * Serializes the given avro object to a byte[].
   */
  public static <T> byte[] toBytes(final T avroObject, final Class<T> theClass) {
    final DatumWriter<T> datumWriter = new SpecificDatumWriter<>(theClass);
    final byte[] theBytes;
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
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
}
