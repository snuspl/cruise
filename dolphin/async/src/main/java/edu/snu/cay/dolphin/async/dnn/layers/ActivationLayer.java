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
import edu.snu.cay.dolphin.async.dnn.blas.function.Function;
import edu.snu.cay.dolphin.async.dnn.blas.function.FunctionFactory;
import edu.snu.cay.dolphin.async.dnn.conf.LayerConfigurationParameters.*;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * Activation layer.
 *
 * This layer applies the specified activation function to each element of an input, maintaining the input's shape.
 * This layer does not have parameters, in other words, is not learnable.
 */
public final class ActivationLayer extends LayerBase {

  private final Function activationFunction;

  private Matrix output;
  private Matrix derivative;

  /**
   * @param index the index of this layer
   * @param inputShape the shape of input data
   * @param activationFunction the type of the activation function
   */
  @Inject
  private ActivationLayer(@Parameter(LayerIndex.class) final int index,
                          @Parameter(LayerInputShape.class) final String inputShape,
                          @Parameter(ActivationFunction.class) final String activationFunction) {
    super(index, inputShape);
    this.activationFunction = FunctionFactory.getSingleInstance(activationFunction);
    this.output = null;
    this.derivative = null;
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
   * Applies the specified activation function.
   * @param input an input value for this layer.
   * @return the activation.
   */
  @Override
  public Matrix feedForward(final Matrix input) {
    if (output == null) {
      output = input.dup();
    } else {
      output.copy(input);
    }

    // apply activation function.
    return activationFunction.applyi(output);
  }

  /**
   * Computes an error for this activation layer.
   * @param input the input value.
   * @param activation the activation value.
   * @param nextError an error of the next layer - the one closer to the output layer.
   * @return an error for this activation layer.
   */
  @Override
  public Matrix backPropagate(final Matrix input, final Matrix activation, final Matrix nextError) {
    if (derivative == null) {
      derivative = input.dup();
    } else {
      derivative.copy(input);
    }

    activationFunction.derivativei(derivative);
    return nextError.mul(derivative);
  }

  /** {@inheritDoc} */
  @Override
  public LayerParameter generateParameterGradient(final Matrix input, final Matrix error) {
    throw new RuntimeException("This layer is not learnable");
  }

  @Override
  public void cleanup() {

  }
}
