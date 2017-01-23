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
 * Created by yunseong on 1/23/17.
 */
final class ChangedTopicCounts implements Copyable<ChangedTopicCounts> {
  private final Table<Integer, Integer, Integer> changedTopicCounts;

  ChangedTopicCounts() {
    this.changedTopicCounts = HashBasedTable.create();
  }

  private ChangedTopicCounts(final Table<Integer, Integer, Integer> changedTopicCounts) {
    this.changedTopicCounts = changedTopicCounts;
  }

  @Override
  public ChangedTopicCounts copyOf() {
    final Table<Integer, Integer, Integer> copied =
        HashBasedTable.create(changedTopicCounts.rowKeySet().size(), changedTopicCounts.columnKeySet().size());
    changedTopicCounts.cellSet().forEach(
        cell -> copied.put(cell.getRowKey(), cell.getColumnKey(), cell.getValue()));
    return new ChangedTopicCounts(copied);
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

  Table<Integer, Integer, Integer> get() {
    return changedTopicCounts;
  }
}
