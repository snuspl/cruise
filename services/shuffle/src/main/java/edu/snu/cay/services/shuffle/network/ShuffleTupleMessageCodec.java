/**
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

import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.wake.remote.Codec;

import javax.inject.Inject;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public final class ShuffleTupleMessageCodec implements StreamingCodec<ShuffleTupleMessage> {

  private Map<String, Map<String, Codec<Tuple>>> tupleCodecMap;

  @Inject
  public ShuffleTupleMessageCodec() {
    tupleCodecMap = new ConcurrentHashMap<>();
  }

  public void registerTupleCodec(
      final String shuffleGroupName, final String shuffleName, final Codec<Tuple> tupleCodec) {
    if (!tupleCodecMap.containsKey(shuffleGroupName)) {
      tupleCodecMap.put(shuffleGroupName, new ConcurrentHashMap<String, Codec<Tuple>>());
    }
    tupleCodecMap.get(shuffleGroupName).put(shuffleName, tupleCodec);
  }

  @Override
  public byte[] encode(final ShuffleTupleMessage msg) {
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      try (final DataOutputStream daos = new DataOutputStream(baos)) {
        encodeToStream(msg, daos);
      }
      return baos.toByteArray();
    } catch (final IOException e) {
      throw new RuntimeException("An IOException occurred in encode method of ShuffleMessageCodec", e);
    }
  }

  @Override
  public ShuffleTupleMessage decode(final byte[] data) {
    try (final ByteArrayInputStream bais = new ByteArrayInputStream(data)) {
      try (final DataInputStream dais = new DataInputStream(bais)) {
        return decodeFromStream(dais);
      }
    } catch (final IOException e) {
      throw new RuntimeException("An IOException occurred in decode method of ShuffleMessageCodec", e);
    }
  }

  @Override
  public void encodeToStream(final ShuffleTupleMessage msg, final DataOutputStream stream) {
    try {
      if (msg.getShuffleGroupName() == null) {
        stream.writeUTF("");
      } else {
        stream.writeUTF(msg.getShuffleGroupName());
      }

      if (msg.getShuffleName() == null) {
        stream.writeUTF("");
      } else {
        stream.writeUTF(msg.getShuffleName());
      }

      stream.writeInt(msg.size());

      final int messageLength = msg.size();
      final Codec<Tuple> tupleCodec = tupleCodecMap.get(msg.getShuffleGroupName()).get(msg.getShuffleName());
      for (int i = 0; i < messageLength; i++) {
        if (tupleCodec instanceof StreamingCodec) {
          ((StreamingCodec<Tuple>)tupleCodec).encodeToStream(msg.get(i), stream);
        } else {
          final byte[] serializedTuple = tupleCodec.encode(msg.get(i));
          stream.writeInt(serializedTuple.length);
          stream.write(serializedTuple);
        }
      }
    } catch(final IOException exception) {
      throw new RuntimeException(exception);
    }
  }

  @Override
  public ShuffleTupleMessage decodeFromStream(final DataInputStream stream) {
    try {
      String shuffleGroupName = stream.readUTF();
      String shuffleName = stream.readUTF();

      if (shuffleGroupName.equals("")) {
        shuffleGroupName = null;
      }

      if (shuffleName.equals("")) {
        shuffleName = null;
      }

      final int dataNum = stream.readInt();

      final List<Tuple> tupleList = new ArrayList<>(dataNum);
      final Codec<Tuple> tupleCodec = tupleCodecMap.get(shuffleGroupName).get(shuffleName);

      for (int i = 0; i < dataNum; i++) {
        if (tupleCodec instanceof StreamingCodec) {
          tupleList.add(((StreamingCodec<Tuple>)tupleCodec).decodeFromStream(stream));
        } else {
          final int length = stream.readInt();
          final byte[] serializedTuple = new byte[length];
          stream.readFully(serializedTuple);
        }
      }

      return new ShuffleTupleMessage(shuffleGroupName, shuffleName, tupleList);
    } catch(final IOException exception) {
      throw new RuntimeException(exception);
    }
  }
}
