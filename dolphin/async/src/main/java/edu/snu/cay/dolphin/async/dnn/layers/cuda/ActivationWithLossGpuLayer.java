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
 * Loss layer with activation function.
 *
 * This layer is intended to be the last layer, in most cases.
 * This layer is not learnable.
 * We use cuDNN library to implement this layer.
 */
public final class ActivationWithLossGpuLayer extends LayerBase {

  private final String lossFunction;
  private MatrixFactory matrixFactory;

  private Pointer inputDesc;
  private Pointer activationDesc;

  /**
   * @param index the index of this layer
   * @param inputShape the shape of input data
   * @param lossFunction the type of the loss function
   * @param activationFunction the type of the activation function
   * @param batchSize the batch Size (number of images) of this layer
   * @param matrixFactory the factory to create new matrices
   */
  @Inject
  private ActivationWithLossGpuLayer(@Parameter(LayerIndex.class) final int index,
                                     @Parameter(LayerInputShape.class) final String inputShape,
                                     @Parameter(LossFunction.class) final String lossFunction,
                                     @Parameter(ActivationFunction.class) final String activationFunction,
                                     @Parameter(NeuralNetworkConfigurationParameters.BatchSize.class)
                                       final int batchSize,
                                     final MatrixFactory matrixFactory) {
    super(index, inputShape);
    this.lossFunction = lossFunction;
    this.matrixFactory = matrixFactory;

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
    if (activationFunction.toLowerCase().equals("softmax")) {
      this.inputDesc = JavaCudnn.createTensorDesc(batchSize, inputChannel, inputHeight, inputWidth);
      JavaCudnn.checkNullPointer(inputDesc);
      this.activationDesc = JavaCudnn.createTensorDesc(batchSize, inputChannel, inputHeight, inputWidth);
      JavaCudnn.checkNullPointer(activationDesc);
    } else {
      throw new IllegalArgumentException("Unsupported activation function");
    }
  }

  @Override
  public int[] getOutputShape() {
    return getInputShape();
  }

  @Override
  public boolean isLearnable() {
    return false;
  }

  @Override
  public Matrix feedForward(final Matrix input) {
    final Matrix output = matrixFactory.create(input.getRows(), input.getColumns());
    if (JavaCudnn.activWithLossFeedForward(inputDesc, ((MatrixCudaImpl) input).getDevicePointer(),
        activationDesc, ((MatrixCudaImpl) output).getDevicePointer())) {
      return output;
    } else {
      throw new RuntimeException("Failed to feedForward");
    }
  }

  /**
   * Compute the error for the specified loss function.
   * @param label the label value.
   * @param activation the activation value.
   * @param nextError an error of the next layer - this argument is ignored.
   * @return the error with respect to the activation and label values.
   */
  @Override
  public Matrix backPropagate(final Matrix label, final Matrix activation, final Matrix nextError) {
    switch (lossFunction.toLowerCase()) {
    case "crossentropy":
      return activation.sub(label);
    default:
      throw new IllegalArgumentException("Unsupported loss function");
    }
  }

  @Override
  public LayerParameter generateParameterGradient(final Matrix input, final Matrix error) {
    throw new RuntimeException("This layer is not learnable");
  }
}
