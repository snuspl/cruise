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
package edu.snu.cay.dolphin.async.dnn.util;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class for storing the validation statistics of a {@link Validator}.
 */
@ThreadSafe
public final class ValidationStats {
  private final AtomicInteger totalNum;
  private final AtomicInteger correctNum;

  public ValidationStats() {
    this(0, 0);
  }

  public ValidationStats(final int totalNum, final int correctNum) {
    this.totalNum = new AtomicInteger(totalNum);
    this.correctNum = new AtomicInteger(correctNum);
  }

  /**
   * An input was classified correctly by the neural network.
   */
  public void validationCorrect() {
    totalNum.incrementAndGet();
    correctNum.incrementAndGet();
  }

  /**
   * An input was classified incorrectly by the neural network.
   */
  public void validationIncorrect() {
    totalNum.incrementAndGet();
  }

  /**
   * Reset statistics.
   */
  public void reset() {
    totalNum.set(0);
    correctNum.set(0);
  }

  /**
   * @return the prediction accuracy of model.
   */
  public float getAccuracy() {
    return correctNum.get() / totalNum.floatValue();
  }

  /**
   * @return the prediction error of model.
   */
  public float getError() {
    return 1 - getAccuracy();
  }

  /**
   * @return the total number of samples that were used for evaluation.
   */
  public int getTotalNum() {
    return totalNum.get();
  }

  /**
   * @return the number of samples that were validated correct.
   */
  public int getCorrectNum() {
    return correctNum.get();
  }
}
