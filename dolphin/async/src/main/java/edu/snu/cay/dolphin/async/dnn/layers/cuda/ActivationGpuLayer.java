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
import edu.snu.cay.dolphin.async.dnn.blas.cuda.MatrixCudaImpl;
import edu.snu.cay.dolphin.async.dnn.conf.LayerConfigurationParameters.*;
import edu.snu.cay.dolphin.async.dnn.conf.NeuralNetworkConfigurationParameters;
import edu.snu.cay.dolphin.async.dnn.layers.LayerBase;
import edu.snu.cay.dolphin.async.dnn.layers.LayerParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.bytedeco.javacpp.Pointer;

import javax.inject.Inject;

/**
 * Gpu activation layer.
 *
 * This layer applies the specified activation function to each element of an input, maintaining the input's shape.
 * This layer does not have parameters, in other words, is not learnable.
 * We use cuDNN library to implement this layer.
 */
public final class ActivationGpuLayer extends LayerBase {

  private Pointer inputDesc;
  private Pointer activationDesc;
  private Pointer activFuncDesc;

  private MatrixFactory matrixFactory;
  private Matrix output;
  private Matrix layerError;

  /**
   * @param index the index of this layer
   * @param inputShape the shape of input data
   * @param activationFunction the type of the activation function
   * @param batchSize the batch Size (number of images) of this layer
   * @param matrixFactory the factory to create new matrices
   */
  @Inject
  private ActivationGpuLayer(@Parameter(LayerIndex.class) final int index,
                             @Parameter(LayerInputShape.class) final String inputShape,
                             @Parameter(ActivationFunction.class) final String activationFunction,
                             @Parameter(NeuralNetworkConfigurationParameters.BatchSize.class) final int batchSize,
                             final MatrixFactory matrixFactory) {
    super(index, inputShape);
    this.matrixFactory = matrixFactory;
    this.output = matrixFactory.create(0, 0);
    this.layerError = matrixFactory.create(0, 0);

    final int inputChannel;
    final int inputHeight;
    final int inputWidth;
    if (getInputShape().length == 2) {
      inputChannel = 1;
      inputHeight = getInputShape()[0];
      inputWidth = getInputShape()[1];
    } else {
      inputChannel = getInputShape()[0];
      inputHeight = getInputShape()[1];
      inputWidth = getInputShape()[2];
    }

    //setup
    this.inputDesc = JavaCudnn.createTensorDesc(batchSize, inputChannel, inputHeight, inputWidth);
    this.activationDesc = JavaCudnn.createTensorDesc(batchSize, inputChannel, inputHeight, inputWidth);
    final char func;
    switch (activationFunction.toLowerCase()) {
    case "sigmoid": func = 'S';
      break;
    case "relu": func = 'R';
      break;
    case "tanh": func = 'T';
      break;
    case "clipped relu": func = 'C';
      break;
    default:
      throw new IllegalArgumentException("Unsupported function: " + activationFunction);
    }
    this.activFuncDesc = JavaCudnn.createActivFuncDesc(func);
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

    if (!output.hasSameSize(input)) {
      output.free();
      output = matrixFactory.create(input.getRows(), input.getColumns());
    }

    if (JavaCudnn.activFeedForward(activFuncDesc, inputDesc, ((MatrixCudaImpl) input).getDevicePointer(),
        activationDesc, ((MatrixCudaImpl) output).getDevicePointer())) {
      return output;
    } else {
      throw new RuntimeException("Failed to feedForward");
    }
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

    if (!layerError.hasSameSize(nextError)) {
      layerError.free();
      layerError = matrixFactory.create(nextError.getRows(), nextError.getColumns());
    }

    if (JavaCudnn.activBackPropagate(activFuncDesc, activationDesc, ((MatrixCudaImpl) activation).getDevicePointer(),
        activationDesc, ((MatrixCudaImpl) nextError).getDevicePointer(),
        inputDesc, ((MatrixCudaImpl) input).getDevicePointer(),
        inputDesc, ((MatrixCudaImpl) layerError).getDevicePointer())) {
      return layerError;
    } else {
      throw new RuntimeException("Failed to backPropagate");
    }
  }

  /** {@inheritDoc} */
  @Override
  public LayerParameter generateParameterGradient(final Matrix input, final Matrix error) {
    throw new RuntimeException("This layer is not learnable");
  }

  @Override
  public void cleanup() {
    JavaCudnn.destroyTensorDesc(inputDesc);
    JavaCudnn.destroyTensorDesc(activationDesc);
    JavaCudnn.destroyActivFuncDesc(activFuncDesc);
  }
}
