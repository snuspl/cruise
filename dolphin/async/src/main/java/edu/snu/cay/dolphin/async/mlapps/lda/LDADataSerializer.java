/*
 * Copyright (C) 2016 Seoul National University
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

import edu.snu.cay.services.em.serialize.Serializer;
import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.*;
import java.util.List;

/**
 * Serializer that provides codec for (de-)serializing data used in LDA.
 */
final class LDADataSerializer implements Serializer {
  private final int numTopics;
  private final LDADataCodec ldaDataCodec = new LDADataCodec();

  @Inject
  private LDADataSerializer(@Parameter(LDAParameters.NumTopics.class) final int numTopics) {
    this.numTopics = numTopics;
  }

  @Override
  public Codec getCodec() {
    return ldaDataCodec;
  }

  private final class LDADataCodec implements Codec<Document>, StreamingCodec<Document> {
    @Override
    public byte[] encode(final Document document) {
      final int numBytes = (document.getWords().size() * 2 + numTopics + 1) * Integer.BYTES;

      try (final ByteArrayOutputStream baos = new ByteArrayOutputStream(numBytes);
           final DataOutputStream daos = new DataOutputStream(baos)) {
        encodeToStream(document, daos);
        return baos.toByteArray();
      } catch (final IOException e) {
        throw new RuntimeException(e.getCause());
      }
    }

    @Override
    public Document decode(final byte[] bytes) {
      try (final DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes))) {
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
        for (int topicIdx = 0; topicIdx < numTopics; topicIdx++) {
          daos.writeInt(document.getTopicCount(topicIdx));
        }
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
        final int[] topicCounts = new int[numTopics];

        for (int i = 0; i < numWords; i++) {
          words[i] = dais.readInt();
        }
        for (int i = 0; i < numWords; i++) {
          assignments[i] = dais.readInt();
        }
        for (int i = 0; i < numTopics; i++) {
          topicCounts[i] = dais.readInt();
        }

        return new Document(words, assignments, topicCounts, numTopics);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
