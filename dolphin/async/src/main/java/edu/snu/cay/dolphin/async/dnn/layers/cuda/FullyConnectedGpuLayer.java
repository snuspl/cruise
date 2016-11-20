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
package edu.snu.cay.dolphin.async.dnn.layers.cuda;

import edu.snu.cay.dolphin.async.dnn.blas.Matrix;
import edu.snu.cay.dolphin.async.dnn.blas.MatrixFactory;
import edu.snu.cay.dolphin.async.dnn.blas.MatrixUtils;
import edu.snu.cay.dolphin.async.dnn.conf.LayerConfigurationParameters;
import edu.snu.cay.dolphin.async.dnn.layerparam.initializer.LayerParameterInitializer;
import edu.snu.cay.dolphin.async.dnn.layers.LayerBase;
import edu.snu.cay.dolphin.async.dnn.layers.LayerParameter;
import edu.snu.cay.dolphin.async.dnn.layers.LayerShape;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

import static edu.snu.cay.dolphin.async.dnn.util.NeuralNetworkUtils.getShapeLength;

/**
 * Fully connected Gpu layer.
 *
 * This layer is learnable having the updatable parameter (weight and bias).
 * We use cuDNN library to implement this layer.
 */
public final class FullyConnectedGpuLayer extends LayerBase {

  private final LayerShape outputShape;
  private final MatrixFactory matrixFactory;
  private Matrix output;
  private Matrix layerError;

  /**
   * @param index the index of this layer
   * @param inputShape the shape of input data
   * @param layerParameterInitializer the layer parameter initializer that generates the new layer parameter following
   *                                  the configuration defined by users
   */
  @Inject
  private FullyConnectedGpuLayer(@Parameter(LayerConfigurationParameters.LayerIndex.class) final int index,
                                 @Parameter(LayerConfigurationParameters.LayerInputShape.class) final String inputShape,
                                 final LayerParameterInitializer layerParameterInitializer,
                                 final MatrixFactory matrixFactory) {
    super(index, inputShape);
    this.outputShape = layerParameterInitializer.getOutputShape();
    this.matrixFactory = matrixFactory;
    this.output = null;
    this.layerError = null;
  }

  /** {@inheritDoc} */
  @Override
  public LayerShape getOutputShape() {
    return outputShape;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isLearnable() {
    return true;
  }

  /**
   * Computes output values for this fully connected layer.
   * @param input input values for this layer.
   * @return output values for this layer.
   */
  @Override
  public Matrix feedForward(final Matrix input) {
    final Matrix weight = getLayerParameter().getWeightParam();
    if (output == null || output.getColumns() != input.getColumns()) {
      output = matrixFactory.create(weight.getRows(), input.getColumns());
    }
    // (output matrix) = (weight matrix) x (input matrix) + (bias column vector)
    return weight.mmul(input, output).addiColumnVector(getLayerParameter().getBiasParam());
  }

  /**
   * Computes errors for this fully connected layer.
   * @param input the input values for this layer.
   * @param activation the output values.
   * @param nextError the errors of the next layer - the one closer to the output layer.
   * @return errors for this layer with the specified input value.
   */
  @Override
  public Matrix backPropagate(final Matrix input, final Matrix activation, final Matrix nextError) {
    final Matrix weight = getLayerParameter().getWeightParam();
    if (layerError == null || layerError.getColumns() != nextError.getColumns()) {
      layerError = matrixFactory.create(weight.getColumns(), nextError.getColumns());
    }
    // (error matrix) = (transposed weight matrix) x (next error matrix)
    return weight.tmmul(nextError, layerError);
  }

  /** {@inheritDoc} */
  @Override
  public LayerParameter generateParameterGradient(final Matrix input, final Matrix nextError) {
    final Matrix weightGradient = matrixFactory.create(getShapeLength(outputShape), getShapeLength(getInputShape()));
    final Matrix biasGradient = matrixFactory.create(getShapeLength(outputShape), 1);
    final LayerParameter parameterGradient = new LayerParameter(weightGradient, biasGradient);

    nextError.mmult(input, parameterGradient.getWeightParam());
    nextError.rowSums(parameterGradient.getBiasParam());
    return parameterGradient;
  }

  @Override
  public void cleanup() {
    super.cleanup();

    MatrixUtils.free(output);
    MatrixUtils.free(layerError);
  }
}
