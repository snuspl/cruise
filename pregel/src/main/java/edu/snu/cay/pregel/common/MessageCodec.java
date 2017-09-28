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

import com.google.common.collect.Lists;
import edu.snu.cay.pregel.PregelParameters.*;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.*;
import java.util.List;

/**
 * Codec for message, which is composed of {@link MessageValueCodec}.
 *
 * Encoding format of message is as follows:
 * [ int: size of message list | M : message value | M : message value | ... ]
 *
 * @param <M> message value
 */
public final class MessageCodec<M> implements Codec<Iterable<M>>, StreamingCodec<Iterable<M>> {

  private final StreamingCodec<M> valueCodec;
  @Inject
  private MessageCodec(@Parameter(MessageValueCodec.class) final StreamingCodec<M> valueCodec) {
    this.valueCodec = valueCodec;
  }

  @Override
  public byte[] encode(final Iterable<M> msgs) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
         DataOutputStream daos = new DataOutputStream(baos)) {
      final List<M> msgList = Lists.newArrayList(msgs);
      final int msgsSize = msgList.size();
      daos.writeInt(msgsSize);
      for (final M message : msgList) {
        daos.write(valueCodec.encode(message));
      }
      return baos.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Iterable<M> decode(final byte[] bytes) {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
         DataInputStream dais = new DataInputStream(bais)) {
      final List<M> msgList = Lists.newArrayList();
      final int size = dais.readInt();
      for (int index = 0; index < size; index++) {
        msgList.add(valueCodec.decodeFromStream(dais));
      }
      return msgList;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void encodeToStream(final Iterable<M> obj, final DataOutputStream stream) {
    throw new NotImplementedException("not implemented");
  }

  @Override
  public Iterable<M> decodeFromStream(final DataInputStream stream) {
    throw new NotImplementedException("not implemented");
  }
}
