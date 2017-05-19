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
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A codec for (de-)serializing data used in LDA application.
 */
final class LDADataCodec implements Codec<Document>, StreamingCodec<Document> {
  private final int numTopics;

  @Inject
  private LDADataCodec(@Parameter(LDAParameters.NumTopics.class) final int numTopics) {
    this.numTopics = numTopics;
  }

  @Override
  public byte[] encode(final Document document) {
    final int numBytes = (document.getWords().size() * 2 + numTopics + 1) * Integer.BYTES;

    try (ByteArrayOutputStream baos = new ByteArrayOutputStream(numBytes);
         DataOutputStream daos = new DataOutputStream(baos)) {
      encodeToStream(document, daos);
      return baos.toByteArray();
    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }
  }

  @Override
  public Document decode(final byte[] bytes) {
    try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes))) {
      return decodeFromStream(dis);
    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }
  }

  @Override
  public void encodeToStream(final Document document, final DataOutputStream daos) {
    try {
      final List<Integer> words = document.getWords();
      daos.writeInt(words.size());
      for (final int word : words) {
        daos.writeInt(word);
      }
      for (int wordIdx = 0; wordIdx < words.size(); wordIdx++) {
        daos.writeInt(document.getAssignment(wordIdx));
      }
      final Map<Integer, Integer> topicCounts = document.getTopicCounts();
      daos.writeInt(topicCounts.size());
      topicCounts.forEach((key, value) -> {
        try {
          daos.writeInt(key);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Document decodeFromStream(final DataInputStream dais) {
    try {
      final int numWords = dais.readInt();
      final int[] words = new int[numWords];
      final int[] assignments = new int[numWords];
      final Map<Integer, Integer> topicCounts = new HashMap<>(numWords);

      for (int wordIdx = 0; wordIdx < numWords; wordIdx++) {
        words[wordIdx] = dais.readInt();
      }
      for (int wordIdx = 0; wordIdx < numWords; wordIdx++) {
        assignments[wordIdx] = dais.readInt();
      }

      final int numEntries = dais.readInt();
      for (int i = 0; i < numEntries; i++) {
        final int topicIdx = dais.readInt();
        final int topicCount = dais.readInt();
        topicCounts.put(topicIdx, topicCount);
      }

      return new Document(words, assignments, topicCounts, numTopics);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
