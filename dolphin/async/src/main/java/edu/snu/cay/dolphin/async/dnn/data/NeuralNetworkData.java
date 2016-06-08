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
package edu.snu.cay.dolphin.async.dnn.data;

import edu.snu.cay.dolphin.async.dnn.blas.Matrix;

/**
 * Data object used for deep neural network jobs.
 */
public final class NeuralNetworkData {

  private final Matrix matrix;
  private final int[] labels;
  private final boolean isValidation;

  /**
   * @param matrix the values of input matrix in which each input instance stored in each row
   * @param labels the labels of input instances
   * @param isValidation the flag indicating whether or not the data batch is for cross validation
   */
  public NeuralNetworkData(final Matrix matrix,
                           final int[] labels,
                           final boolean isValidation) {
    this.matrix = matrix;
    this.labels = labels;
    this.isValidation = isValidation;
  }

  public Matrix getMatrix() {
    return this.matrix;
  }

  public int[] getLabels() {
    return this.labels;
  }

  public boolean isValidation() {
    return this.isValidation;
  }
}
