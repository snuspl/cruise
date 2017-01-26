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

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import edu.snu.cay.utils.Copyable;

/**
 * Encapsulates the changes of topic assignment of each word.
 * Note that this is not thread-safe for performance reason.
 */
final class TopicChanges implements Copyable<TopicChanges> {
  private final Table<Integer, Integer, Integer> changedTopicCounts;

  TopicChanges() {
    this.changedTopicCounts = HashBasedTable.create();
  }

  private TopicChanges(final Table<Integer, Integer, Integer> changedTopicCounts) {
    this.changedTopicCounts = changedTopicCounts;
  }

  @Override
  public TopicChanges copyOf() {
    final Table<Integer, Integer, Integer> copied =
        HashBasedTable.create(changedTopicCounts.rowKeySet().size(), changedTopicCounts.columnKeySet().size());
    changedTopicCounts.cellSet().forEach(
        cell -> copied.put(cell.getRowKey(), cell.getColumnKey(), cell.getValue()));
    return new TopicChanges(copied);
  }

  /**
   * Increments the number of assignment of a word to a topic
   *
   * @param word a word
   * @param topicIndex a topic index
   * @param delta a number of changes to make
   */
  void increment(final int word, final int topicIndex, final int delta) {
    if (!changedTopicCounts.contains(word, topicIndex)) {
      changedTopicCounts.put(word, topicIndex, 0);
    }
    changedTopicCounts.put(word, topicIndex, delta + changedTopicCounts.get(word, topicIndex));
  }

  /**
   * Changes the topic assigned to a word.
   * @param word a word
   * @param oldTopicIdx topic index that was previously assigned to the word
   * @param newTopicIdx topic index that will be assigned to the word
   * @param delta a number of changes to make
   */
  void replace(final int word, final int oldTopicIdx, final int newTopicIdx, final int delta) {
    if (!changedTopicCounts.contains(word, oldTopicIdx)) {
      changedTopicCounts.put(word, oldTopicIdx, 0);
    }
    changedTopicCounts.put(word, oldTopicIdx, changedTopicCounts.get(word, oldTopicIdx) - delta);

    if (!changedTopicCounts.contains(word, newTopicIdx)) {
      changedTopicCounts.put(word, newTopicIdx, 0);
    }
    changedTopicCounts.put(word, newTopicIdx, changedTopicCounts.get(word, newTopicIdx) + delta);
  }

  /**
   * Returns a table, where each element at (WordIdx, TopicIdx) is the number of changes for a word to a topic.
   * For example, if the topic of a word w0 is changed its assignment to t0 at n times, then T[w0, t0] = n.
   * @return The number of topic changes as a form of Table.
   */
  Table<Integer, Integer, Integer> getTable() {
    return changedTopicCounts;
  }
}
