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
package edu.snu.cay.services.shuffle.network;

import org.apache.reef.io.network.impl.StreamingCodec;

import javax.inject.Inject;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Codec for ShuffleControlMessage.
 */
final class ShuffleControlMessageCodec implements StreamingCodec<ShuffleControlMessage> {

  @Inject
  private ShuffleControlMessageCodec() {
  }

  @Override
  public byte[] encode(final ShuffleControlMessage msg) {
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      try (final DataOutputStream daos = new DataOutputStream(baos)) {
        encodeToStream(msg, daos);
      }
      return baos.toByteArray();
    } catch (final IOException e) {
      throw new RuntimeException("An IOException occurred while encoding ShuffleControlMessage", e);
    }
  }

  @Override
  public ShuffleControlMessage decode(final byte[] data) {
    try (final ByteArrayInputStream bais = new ByteArrayInputStream(data)) {
      try (final DataInputStream dais = new DataInputStream(bais)) {
        return decodeFromStream(dais);
      }
    } catch (final IOException e) {
      throw new RuntimeException("An IOException occurred while decoding ShuffleControlMessage", e);
    }
  }

  @Override
  public void encodeToStream(final ShuffleControlMessage msg, final DataOutputStream stream) {
    try {
      stream.writeInt(msg.getCode());

      final int messageLength = msg.size();

      stream.writeInt(messageLength);
      for (int i = 0; i < messageLength; i++) {
        final String endPointId = msg.get(i);
        stream.writeUTF(endPointId);
      }
    } catch (final IOException exception) {
      throw new RuntimeException(exception);
    }
  }

  @Override
  public ShuffleControlMessage decodeFromStream(final DataInputStream stream) {
    try {
      final int code = stream.readInt();
      final int messageLength = stream.readInt();

      final List<String> endPointIdList = new ArrayList<>(messageLength);

      for (int i = 0; i < messageLength; i++) {
        endPointIdList.add(stream.readUTF());
      }

      return new ShuffleControlMessage(code, endPointIdList);
    } catch (final IOException exception) {
      throw new RuntimeException(exception);
    }
  }
}
