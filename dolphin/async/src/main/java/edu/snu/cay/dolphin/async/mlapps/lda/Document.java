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

import com.google.common.primitives.Ints;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Representation of a document in a corpus. This has words and corresponding topic assignment
 * in the document as well as a document-topic assignment table of the document.
 */
final class Document {

  private final List<Integer> words;
  private final int[] assignments;
  private final Map<Integer, Integer> topicCounts;
  private final int numTopics;

  /**
   * Creates a document with given words. The initial topics for the words are assigned randomly.
   * @param words Words that the document contains
   * @param numTopics Number of topics determined by user parameter
   *                  ({@link edu.snu.cay.dolphin.async.mlapps.lda.LDAParameters.NumTopics})
   */
  Document(final int[] words, final int numTopics) {
    this.words = Ints.asList(words);
    this.assignments = new int[words.length];
    this.topicCounts = new HashMap<>(words.length); // the number of assigned topics is bound to the document's words
    this.numTopics = numTopics;

    initialize();
  }

  /**
   * Creates a document with words and intermediate topic assignments that have been learned.
   * @param words Words that the document contains
   * @param assignments Topic Index that a word is assigned to
   * @param topicCounts Number of words that are assigned to a topic
   * @param numTopics Number of topics determined by user parameter
   *                  ({@link edu.snu.cay.dolphin.async.mlapps.lda.LDAParameters.NumTopics})
   */
  Document(final int[] words, final int[] assignments, final Map<Integer, Integer> topicCounts, final int numTopics) {
    this.words = Ints.asList(words);
    this.assignments = assignments;
    this.topicCounts = topicCounts;
    this.numTopics = numTopics;
  }

  /**
   * Assigns each word in the doc to a random topic.
   */
  private void initialize() {
    final Random rand = new Random();
    for (int i = 0; i < assignments.length; i++) {
      final int topic = rand.nextInt(numTopics);
      assignments[i] = topic;
      topicCounts.compute(topic, (key, oldValue) -> {
        if (oldValue == null) {
          return 1;
        } else {
          return oldValue + 1;
        }
      });
    }
  }

  int size() {
    return words.size();
  }

  int getWord(final int index) {
    return words.get(index);
  }

  List<Integer> getWords() {
    return words;
  }

  /**
   * @param index Index of the word
   * @return Topic Index that the word is assigned to
   */
  int getAssignment(final int index) {
    return assignments[index];
  }

  void removeWordAtIndex(final int index) {
    final int oldTopic = assignments[index];
    topicCounts.compute(oldTopic, (key, oldValue) -> {
      if (oldValue == null || oldValue == 0) {
        throw new RuntimeException(String.format("The TopicCounts for %d-th word is %d", index, oldValue));
      } else {
        return oldValue - 1;
      }
    });
  }

  void addWordAtIndex(final int index, final int newTopic) {
    assignments[index] = newTopic;
    topicCounts.compute(newTopic, (key, oldValue) -> {
      if (oldValue == null) {
        return 1;
      } else {
        return oldValue + 1;
      }
    });
  }

  void setTopicCount(final int topicIdx, final int value) {
    topicCounts.put(topicIdx, value);
  }

  /**
   * @param topicIndex Index of a topic
   * @return Number of words that are assigned to the topic
   */
  int getTopicCount(final int topicIndex) {
    return topicCounts.getOrDefault(topicIndex, 0);
  }

  Map<Integer, Integer> getTopicCounts() {
    return topicCounts;
  }
}
