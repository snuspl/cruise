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

    for (int i = 0; i < outputMatrix.getColumns(); ++i) {
      final Matrix output = outputMatrix.getColumn(i);
      float maxValue = output.get(0);

      // Find the index with highest probability.
      int maxIndex = 0;
      for (int j = 1; j < output.getLength(); ++j) {
        if (output.get(j) > maxValue) {
          maxValue = output.get(j);
          maxIndex = j;
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
