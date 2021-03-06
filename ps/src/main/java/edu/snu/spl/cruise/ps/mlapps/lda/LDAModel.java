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
package edu.snu.spl.cruise.ps.mlapps.lda;

import edu.snu.spl.cruise.utils.Copyable;

import java.util.Map;

/**
 * Encapsulates the model data in LDA app.
 */
public final class LDAModel implements Copyable<LDAModel> {
  private final int[] topicSummaryVector;
  private final Map<Integer, int[]> wordTopicVectors;
  private final TopicChanges topicChanges;

  LDAModel(final int[] topicSummaryVector,
           final Map<Integer, int[]> wordTopicVectors) {
    this.topicSummaryVector = topicSummaryVector;
    this.wordTopicVectors = wordTopicVectors;
    this.topicChanges = new TopicChanges();
  }

  private LDAModel(final int[] topicSummaryVector,
                   final Map<Integer, int[]> wordTopicVectors,
                   final TopicChanges topicChanges) {
    this.topicSummaryVector = topicSummaryVector;
    this.wordTopicVectors = wordTopicVectors;
    this.topicChanges = topicChanges;
  }

  int[] getTopicSummaryVector() {
    return topicSummaryVector;
  }

  Map<Integer, int[]> getWordTopicVectors() {
    return wordTopicVectors;
  }

  TopicChanges getTopicChanges() {
    return topicChanges;
  }

  @Override
  public LDAModel copyOf() {
    return new LDAModel(topicSummaryVector, wordTopicVectors, topicChanges.copyOf());
  }
}
