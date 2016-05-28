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
package edu.snu.cay.dolphin.examples.ml.data;

import edu.snu.cay.dolphin.examples.ml.sub.IntegerListCodec;
import edu.snu.cay.services.em.serialize.Serializer;
import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;
import java.util.List;

/**
 * A serializer for Dolphin jobs that takes in {@code List<Integer>} as the adjacency list of a graph vertex.
 * For example, use this if the job only uses data from {@link AdjacencyListParser}, stored as
 * (key = vertex, value = list of adjacent vertices).
 */
public final class AdjacencyListSerializer implements Serializer {
  private final Codec<List<Integer>> integerListCodec;

  @Inject
  private AdjacencyListSerializer(final IntegerListCodec integerListCodec) {
    this.integerListCodec = integerListCodec;
  }

  @Override
  public Codec getCodec() {
    return integerListCodec;
  }
}
