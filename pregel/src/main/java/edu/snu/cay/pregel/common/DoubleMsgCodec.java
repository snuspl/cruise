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
 * Codec for message list which element type is double.
 */
public final class DoubleMsgCodec implements Codec<Iterable<Double>> {

  @Inject
  private DoubleMsgCodec() {
  }

  @Override
  public byte[] encode(final Iterable<Double> msgs) {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream(getNumBytes(msgs));
    final DataOutputStream daos = new DataOutputStream(baos);
    final List<Double> msgList = Lists.newArrayList(msgs);
    final int msgsSize = msgList.size();
    try {
      daos.writeInt(msgsSize);
      for (final Double msg : msgList) {
        daos.writeDouble(msg);
      }

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return baos.toByteArray();
  }

  @Override
  public Iterable<Double> decode(final byte[] bytes) {
    final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    final DataInputStream dais = new DataInputStream(bais);
    final List<Double> msgList = Lists.newArrayList();
    try {

      final int size = dais.readInt();
      for (int index = 0; index < size; index++) {
        msgList.add(dais.readDouble());
      }

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return msgList;
  }

  private int getNumBytes(final Iterable<Double> doubles) {
    return Integer.BYTES + Double.BYTES * Iterables.size(doubles);
  }
}
