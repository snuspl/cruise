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
package edu.snu.cay.async.examples.lda;

import edu.snu.cay.services.ps.worker.api.ParameterWorker;

import java.util.HashMap;
import java.util.Map;

/**
 * Updates many changed topic changes at once.
 */
final class LdaBatchParameterWorker {

  private final ParameterWorker<Integer, int[], int[]> parameterWorker;
  private final int numTopics;
  private final Map<Integer, int[]> changedTopicCounts;

  LdaBatchParameterWorker(final ParameterWorker<Integer, int[], int[]> parameterWorker,
                          final int numTopics) {
    this.parameterWorker = parameterWorker;
    this.numTopics = numTopics;
    this.changedTopicCounts = new HashMap<>();
  }

  void addTopicChange(final int word, final int topicIndex, final int delta) {
    if (!changedTopicCounts.containsKey(word)) {
      changedTopicCounts.put(word, new int[numTopics]);
    }

    final int[] changedTopicCountsForWord = changedTopicCounts.get(word);
    changedTopicCountsForWord[topicIndex] += delta;
  }

  void pushAndClear() {
    final int[] changedTopicIndices = new int[numTopics];
    for (final int changedWord : changedTopicCounts.keySet()) {
      final int[] changedTopicCountsForWord = changedTopicCounts.get(changedWord);
      int numChangedTopics = 0;
      for (int i = 0; i < changedTopicCountsForWord.length; i++) {
        if (changedTopicCountsForWord[i] != 0) {
          changedTopicIndices[numChangedTopics] = i;
          numChangedTopics++;
        }
      }

      final int[] parameters = new int[2 * numChangedTopics];
      for (int i = 0; i < numChangedTopics; i++) {
        final int changedTopicIndex = changedTopicIndices[i];
        parameters[2 * i] = changedTopicIndex;
        parameters[2 * i + 1] = changedTopicCountsForWord[changedTopicIndex];
      }

      parameterWorker.push(changedWord, parameters);
    }

    changedTopicCounts.clear();
  }
}
