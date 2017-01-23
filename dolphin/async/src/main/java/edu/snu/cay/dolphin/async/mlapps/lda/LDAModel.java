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

import edu.snu.cay.utils.Copyable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by yunseong on 1/18/17.
 */
public final class LDAModel implements Copyable<LDAModel> {
  private final int[] topicSummaryVector;
  private final Map<Integer, int[]> wordTopicVectors;
  private final ChangedTopicCounts changedTopicCounts;

  LDAModel(final int[] topicSummaryVector,
           final Map<Integer, int[]> wordTopicVectors) {
    this.topicSummaryVector = topicSummaryVector;
    this.wordTopicVectors = wordTopicVectors;
    this.changedTopicCounts = new ChangedTopicCounts();
  }

  private LDAModel(final int[] topicSummaryVector,
                   final Map<Integer, int[]> wordTopicVectors,
                   final ChangedTopicCounts changedTopicCounts) {
    this.topicSummaryVector = topicSummaryVector;
    this.wordTopicVectors = wordTopicVectors;
    this.changedTopicCounts = changedTopicCounts;
  }

  int[] getTopicSummaryVector() {
    return topicSummaryVector;
  }

  Map<Integer, int[]> getWordTopicVectors() {
    return wordTopicVectors;
  }

  ChangedTopicCounts getChangedTopicCounts() {
    return changedTopicCounts;
  }

  @Override
  public LDAModel copyOf() {
    final int[] topicSummaryVectorCopy = Arrays.copyOf(topicSummaryVector, topicSummaryVector.length);
    final Map<Integer, int[]> wordTopicVectorsCopy = new HashMap<>(wordTopicVectors.size());
    wordTopicVectors.forEach((k, v) -> wordTopicVectorsCopy.put(k, v.clone()));
    return new LDAModel(topicSummaryVectorCopy, wordTopicVectorsCopy, changedTopicCounts.copyOf());
  }
}
