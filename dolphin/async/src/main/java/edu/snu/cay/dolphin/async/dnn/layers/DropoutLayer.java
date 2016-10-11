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
package edu.snu.cay.dolphin.async.dnn.layers;

import edu.snu.cay.dolphin.async.dnn.blas.Matrix;
import edu.snu.cay.dolphin.async.dnn.blas.MatrixFactory;
import edu.snu.cay.dolphin.async.dnn.conf.LayerConfigurationParameters.*;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * Dropout layer.
 *
 * This layer prevents neural networks from overfitting,
 * by randomly dropping units (along with their connections) from the neural network during training.
 */
public final class DropoutLayer extends LayerBase {

  private final MatrixFactory matrixFactory;
  private final float dropoutRatio;
  private Matrix bernoulliMatrix;

  /**
   * @param index the index of this layer.
   * @param inputShape the shape of input data.
   * @param dropoutRatio the failure probability for bernoulli matrix.
   * @param matrixFactory the factory to create bernoulli matrix.
   */
  @Inject
  private DropoutLayer(@Parameter(LayerIndex.class) final int index,
                       @Parameter(LayerInputShape.class) final String inputShape,
                       @Parameter(DropoutRatio.class) final float dropoutRatio,
                       final MatrixFactory matrixFactory) {
    super(index, inputShape);
    this.dropoutRatio = dropoutRatio;
    this.matrixFactory = matrixFactory;
  }

  @Override
  public int[] getOutputShape() {
    return getInputShape();
  }

  /** {@inheritDoc} */
  @Override
  public boolean isLearnable() {
    return false;
  }

  /**
   * Computes output values for this dropout layer.
   * @param input input values for this layer.
   * @return output values for this layer.
   */
  @Override
  public Matrix feedForward(final Matrix input) {
    this.bernoulliMatrix = matrixFactory
        .bernoulli(input.getRows(), input.getColumns(), 1 - dropoutRatio, 1 / (1 - dropoutRatio));
    return bernoulliMatrix.mul(input);
  }

  /**
   * Computes errors.
   * @param input the input values for this layer.
   * @param activation the output values.
   * @param nextError the errors of the next layer - the one closer to the output layer.
   * @return errors for this layer with the specified input value.
   */
  @Override
  public Matrix backPropagate(final Matrix input,
                              final Matrix activation,
                              final Matrix nextError) {
    return bernoulliMatrix.mul(nextError);
  }

  /** {@inheritDoc} */
  @Override
  public LayerParameter generateParameterGradient(final Matrix input, final Matrix error) {
    throw new RuntimeException("This layer is not learnable");
  }
}
