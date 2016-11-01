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

import edu.snu.cay.dolphin.async.dnn.NeuralNetwork;
import edu.snu.cay.dolphin.async.dnn.blas.Matrix;

/**
 * Class for validating a neural network model using a given data input.
 * Calculates the prediction accuracy for the given validation data set.
 */
public final class Validator {
  private final NeuralNetwork network;
  private final ValidationStats validationStats;

  public Validator(final NeuralNetwork network) {
    this.network = network;
    this.validationStats = new ValidationStats();
  }

  public void validate(final Matrix input, final int[] labels) {
    final Matrix[] activations = network.feedForward(input);
    final Matrix outputMatrix = activations[activations.length - 1];
    final float[] outputArray = outputMatrix.toFloatArray();
    final int rows = outputMatrix.getRows();
    final int columns = outputMatrix.getColumns();

    for (int i = 0; i < columns; ++i) {
      float maxValue = Float.MIN_VALUE;
      int maxIndex = -1;

      for (int j = rows * i; j < rows * (i + 1); ++j) {
        if (outputArray[j] > maxValue) {
          maxValue = outputArray[j];
          maxIndex = j % rows;
        }
      }

      if (maxIndex == labels[i]) {
        validationStats.validationCorrect();
      } else {
        validationStats.validationIncorrect();
      }
    }
  }

  public ValidationStats getValidationStats() {
    return validationStats;
  }
}
