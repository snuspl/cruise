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
package edu.snu.cay.dolphin.bsp.examples.matmul;

import org.apache.reef.wake.remote.Codec;

import javax.inject.Inject;
import java.nio.ByteBuffer;

public final class IntegerCodec implements Codec<Integer>, org.apache.reef.io.serialization.Codec<Integer> {

  @Inject
  private IntegerCodec() {
  }

  @Override
  public Integer decode(final byte[] bytes) {
    final ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    return byteBuffer.getInt();
  }

  @Override
  public byte[] encode(final Integer integer) {
    final ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE);
    byteBuffer.putInt(integer);
    return byteBuffer.array();
  }
}
