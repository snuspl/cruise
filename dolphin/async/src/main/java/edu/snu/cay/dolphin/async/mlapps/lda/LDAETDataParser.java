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

import edu.snu.cay.services.et.evaluator.api.DataParser;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

final class LDAETDataParser implements DataParser<Document> {
  private final int numTopics;

  @Inject
  private LDAETDataParser(@Parameter(LDAParameters.NumTopics.class) final int numTopics) {
    this.numTopics = numTopics;
  }

  @Override
  public List<Document> parse(final Collection<String> rawData) {
    final List<Document> result = new LinkedList<>();

    for (final String value : rawData) {
      final String line = value.trim();
      if (line.startsWith("#") || line.length() == 0) {
        continue;
      }

      final String[] split = line.split("\\s+");

      final int[] words = new int[split.length];
      for (int i = 0; i < split.length; i++) {
        words[i] = Integer.parseInt(split[i]);
      }

      result.add(new Document(words, numTopics));
    }

    return result;
  }
}
