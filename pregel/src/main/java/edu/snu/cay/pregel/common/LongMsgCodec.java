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
package edu.snu.cay.pregel.common;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;
import java.io.*;
import java.util.List;

/**
 * Codec for message list which element type is long.
 */
public final class LongMsgCodec implements Codec<Iterable<Long>> {

  @Inject
  private LongMsgCodec() {
  }

  @Override
  public byte[] encode(final Iterable<Long> msgs) {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream(getNumBytes(msgs));
    final DataOutputStream daos = new DataOutputStream(baos);
    final List<Long> msgList = Lists.newArrayList(msgs);
    final int msgsSize = msgList.size();
    try {
      daos.writeInt(msgsSize);
      for (final Long msg : msgList) {
        daos.writeLong(msg);
      }

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return baos.toByteArray();
  }

  @Override
  public Iterable<Long> decode(final byte[] bytes) {
    final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    final DataInputStream dais = new DataInputStream(bais);
    final List<Long> msgList = Lists.newArrayList();
    try {

      final int size = dais.readInt();
      for (int index = 0; index < size; index++) {
        msgList.add(dais.readLong());
      }

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return msgList;
  }

  private int getNumBytes(final Iterable<Long> longs) {
    return Integer.BYTES + Long.BYTES * Iterables.size(longs);
  }
}
