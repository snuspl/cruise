/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.cay.dolphin.async.mlapps.lda;

import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;
import java.io.*;

/**
 * Codec for sparse integer array.
 * This codec is highly optimized for use case in LDA algorithm,
 * which represents model in different form between worker and server.
 */
final class SparseArrayCodec implements Codec<int[]>, StreamingCodec<int[]> {
  @Inject
  private SparseArrayCodec() {
  }

  @Override
  public byte[] encode(final int[] array) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream(getNumBytes(array));
         DataOutputStream dos = new DataOutputStream(baos)) {
      encodeToStream(array, dos);
      return baos.toByteArray();
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Encode model stored in server and write to output stream.
   * i-th element of the input array represents number of assignments of i-th topic,
   * while the last element of the input array represents number of nonzero elements in the array.
   * Do not encode zeros to save network bandwidth.
   * @param array model stored in server
   * @param dos output stream to which this codec writes
   */
  @Override
  public void encodeToStream(final int[] array, final DataOutputStream dos) {
    try {
      dos.writeInt(array[array.length - 1]);
      for (int i = 0; i < array.length - 1; ++i) {
        if (array[i] != 0) {
          dos.writeInt(i);
          dos.writeInt(array[i]);
        }
      }
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int[] decode(final byte[] bytes) {
    try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes))) {
      return decodeFromStream(dis);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Decode byte array sent from server and construct model for worker.
   * The returning array only contains nonzero elements.
   * More precisely, (2 * i)-th element of the array represents a topic index with nonzero value,
   * and corresponding (2 * i + 1)-th element of the array represents
   * number of assignments of topic index which described above.
   * @param dis input stream from which this codec reads
   * @return model for worker
   */
  @Override
  public int[] decodeFromStream(final DataInputStream dis) {
    try {
      final int numNonZeros = dis.readInt();
      final int[] result = new int[2 * numNonZeros];
      for (int i = 0; i < 2 * numNonZeros; ++i) {
        result[i] = dis.readInt();
      }
      return result;
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  int getNumBytes(final int[] array) {
    return Integer.BYTES + 2 * Integer.BYTES * array[array.length - 1];
  }
}
