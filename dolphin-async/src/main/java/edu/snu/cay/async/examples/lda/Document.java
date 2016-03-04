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

import java.util.Random;

/**
 * Representation of a document in a corpus. This has words and corresponding topic assignment
 * in the document as well as a document-topic assignment table of the document.
 */
final class Document {

  private final int[] words;
  private final int[] assignments;
  private final int[] topicCounts;
  private final int numTopics;

  Document(final int[] words, final int numTopics) {
    this.words = words;
    this.assignments = new int[words.length];
    this.topicCounts = new int[numTopics];
    this.numTopics = numTopics;

    initialize();
  }

  private void initialize() {
    final Random rand = new Random();
    for (int i = 0; i < assignments.length; i++) {
      final int topic = rand.nextInt(numTopics);
      assignments[i] = topic;
      topicCounts[topic]++;
    }
  }

  int size() {
    return words.length;
  }

  int getWord(final int index) {
    return words[index];
  }

  int getAssignment(final int index) {
    return assignments[index];
  }

  void removeWord(final int index) {
    final int oldTopic = assignments[index];
    topicCounts[oldTopic]--;
  }

  void addWord(final int index, final int newTopic) {
    assignments[index] = newTopic;
    topicCounts[newTopic]++;
  }

  int getTopicCount(final int topicIndex) {
    return topicCounts[topicIndex];
  }
}
