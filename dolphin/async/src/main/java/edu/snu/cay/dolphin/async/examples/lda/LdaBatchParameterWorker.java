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
package edu.snu.cay.dolphin.async.examples.lda;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;

import javax.inject.Inject;
import java.util.Map;

/**
 * Updates many topic changes at once.
 * Note that this is not thread-safe for performance reason.
 */
final class LdaBatchParameterWorker {

  private final ParameterWorker<Integer, int[], int[]> parameterWorker;
  private final Table<Integer, Integer, Integer> changedTopicCounts;

  @Inject
  private LdaBatchParameterWorker(final ParameterWorker<Integer, int[], int[]> parameterWorker) {
    this.parameterWorker = parameterWorker;
    this.changedTopicCounts = HashBasedTable.create();
  }

  /**
   * Add topic changes that will be pushed later.
   * Note that this is not thread-safe for performance reason.
   *
   * @param word a word
   * @param topicIndex a topic index
   * @param delta changed number of assignment
   */
  void addTopicChange(final int word, final int topicIndex, final int delta) {
    if (!changedTopicCounts.contains(word, topicIndex)) {
      changedTopicCounts.put(word, topicIndex, 0);
    }

    changedTopicCounts.put(word, topicIndex, delta + changedTopicCounts.get(word, topicIndex));
  }

  /**
   * Push all added changes and clear the changes.
   * Note that this is not thread-safe for performance reason.
   */
  void pushAndClear() {
    for (final int changedWord : changedTopicCounts.rowKeySet()) {
      final Map<Integer, Integer> changedTopicCountsForWord = changedTopicCounts.row(changedWord);
      final int numChangedTopics = changedTopicCountsForWord.size();

      // Given a word, an even index represents a changed topic index and a corresponding odd index represents
      // a changed value for the topic index.
      final int[] parameters = new int[2 * numChangedTopics];
      int i = 0;
      for (final Map.Entry<Integer, Integer> entry : changedTopicCountsForWord.entrySet()) {
        parameters[2 * i] = entry.getKey();
        parameters[2 * i + 1] = entry.getValue();
        i++;
      }

      parameterWorker.push(changedWord, parameters);
    }

    changedTopicCounts.clear();
  }
}
