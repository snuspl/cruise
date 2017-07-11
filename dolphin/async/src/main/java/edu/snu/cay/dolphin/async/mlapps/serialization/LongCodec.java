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
package edu.snu.cay.dolphin.async.mlapps.serialization;

import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A codec for Integer.
 */
public final class LongCodec implements Codec<Long>, StreamingCodec<Long> {

  @Inject
  private LongCodec() {
  }

  @Override
  public byte[] encode(final Long obj) {
    final ByteBuffer byteBuffer = ByteBuffer.allocate(Long.BYTES);
    byteBuffer.putLong(obj);
    return byteBuffer.array();
  }


  @Override
  public Long decode(final byte[] bytes) {
    final ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    return byteBuffer.getLong();
  }

  @Override
  public void encodeToStream(final Long obj, final DataOutputStream dos) {
    try {
      dos.writeLong(obj);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Long decodeFromStream(final DataInputStream dis) {
    try {
      return dis.readLong();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
