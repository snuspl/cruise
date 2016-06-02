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
package edu.snu.cay.dolphin.bsp.mlapps.sub;

import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Codec for encoding and decoding an Integer List.
 */
public final class IntegerListCodec implements Codec<List<Integer>>, StreamingCodec<List<Integer>> {

  @Inject
  private IntegerListCodec() {
  }

  @Override
  public byte[] encode(final List<Integer> list) {
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream(getNumBytes(list));
         final DataOutputStream daos = new DataOutputStream(baos)) {
      encodeToStream(list, daos);
      return baos.toByteArray();
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void encodeToStream(final List<Integer> list, final DataOutputStream daos) {
    try {
      daos.writeInt(list.size());
      for (final int item : list) {
        daos.writeInt(item);
      }
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<Integer> decode(final byte[] bytes) {
    try (final DataInputStream dais = new DataInputStream(new ByteArrayInputStream(bytes))) {
      return decodeFromStream(dais);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<Integer> decodeFromStream(final DataInputStream dais) {
    try {
      final int size = dais.readInt();
      final List<Integer> list = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        list.add(dais.readInt());
      }
      return list;
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  public int getNumBytes(final List<Integer> list) {
    return Integer.SIZE + Integer.SIZE * list.size();
  }
}
