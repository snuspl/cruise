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
package edu.snu.cay.dolphin.async.dnn.layerparam.initializer;

import edu.snu.cay.dolphin.async.dnn.blas.Matrix;
import edu.snu.cay.dolphin.async.dnn.blas.MatrixFactory;
import edu.snu.cay.dolphin.async.dnn.conf.LayerConfigurationParameters.*;
import edu.snu.cay.dolphin.async.dnn.layers.LayerParameter;
import edu.snu.cay.dolphin.async.dnn.layers.LayerShape;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

import static edu.snu.cay.dolphin.async.dnn.util.NeuralNetworkUtils.getShapeLength;
import static edu.snu.cay.dolphin.async.dnn.util.NeuralNetworkUtils.shapeFromString;

/**
 * Parameter Initializer of fully connected layer.
 *
 * This class initializes the weight matrix
 * with pseudo random normal distributed value with mean 0 and given standard deviation.
 * This class initializes the bias vector with the given value.
 */
public final class FullyConnectedLayerParameterInitializer implements LayerParameterInitializer {

  private final MatrixFactory matrixFactory;
  private final int index;
  private final int numInput;
  private final int numOutput;
  private final float initWeight;
  private final float initBias;

  /**
   * @param matrixFactory the factory to create new matrices
   * @param index the index of this layer
   * @param inputShape the shape of input data
   * @param numOutput the number of output neurons
   * @param initWeight the standard deviation that is used to initialize the weights from a Gaussian distribution
   *                   with mean {@code 0.0}
   * @param initBias constant value with which the biases of this layer are initialized
   */
  @Inject
  public FullyConnectedLayerParameterInitializer(final MatrixFactory matrixFactory,
                                                 @Parameter(LayerIndex.class) final int index,
                                                 @Parameter(LayerInputShape.class) final String inputShape,
                                                 @Parameter(NumberOfOutput.class) final int numOutput,
                                                 @Parameter(InitialWeight.class) final float initWeight,
                                                 @Parameter(InitialBias.class) final float initBias) {
    this.matrixFactory = matrixFactory;
    this.index = index;
    this.numInput = getShapeLength(shapeFromString(inputShape));
    this.numOutput = numOutput;
    this.initWeight = initWeight;
    this.initBias = initBias;
  }

  /** {@inheritDoc} */
  @Override
  public LayerParameter generateInitialParameter() {
    final Matrix weight = matrixFactory.randn(numOutput, numInput);
    final Matrix bias = matrixFactory.create(numOutput).fill(initBias);

    weight.muli(initWeight); // multiply by standard deviation.

    return new LayerParameter(weight, bias);
  }

  /** {@inheritDoc} */
  @Override
  public int getIndex() {
    return this.index;
  }

  @Override
  public LayerShape getOutputShape() {
    return new LayerShape(numOutput, 1, 1);
  }
}
