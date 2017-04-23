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

import edu.snu.cay.dolphin.async.mlapps.lda.LDAParameters.NumTopics;
import edu.snu.cay.services.et.evaluator.api.UpdateFunction;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Each vector with a word index represents a count vector whose elements are the
 * number of words in the corpus that are assigned to a specific topic. We use
 * numVocabs-th row as the total count vector for all word indices.
 * Note that the last (numTopics-th) element of the array represents number of non-zero elements in the array.
 */
public final class LDAETModelUpdateFunction implements UpdateFunction<Integer, int[], int[]> {
  private final int numTopics;

  @Inject
  private LDAETModelUpdateFunction(@Parameter(NumTopics.class) final int numTopics) {
    this.numTopics = numTopics;
  }

  @Override
  public int[] initValue(final Integer integer) {
    return new int[numTopics + 1];
  }

  @Override
  public int[] updateValue(final Integer integer, final int[] oldValue, final int[] deltaValue) {
    // deltaValue consists of pairs of a word index and a changed value.
    int i = 0;
    while (i < deltaValue.length) {
      final int index = deltaValue[i++];
      final int oldCount = oldValue[index];
      oldValue[index] += deltaValue[i++];

      // Topic assignments should be non-negative, so the negative result is replaced to zero.
      if (oldValue[index] < 0) {
        oldValue[index] = 0;
      }

      // should care about the case when the value is changed from non-zero to zero, vice versa
      if (oldCount == 0 && oldValue[index] != 0) {
        oldValue[numTopics]++;
      } else if (oldCount != 0 && oldValue[index] == 0) {
        oldValue[numTopics]--;
      }
    }
    return oldValue;
  }
}
