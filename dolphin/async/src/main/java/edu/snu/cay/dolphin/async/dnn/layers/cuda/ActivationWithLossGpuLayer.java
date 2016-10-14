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
import edu.snu.cay.dolphin.async.dnn.blas.cuda.MatrixCudaImpl;
import edu.snu.cay.dolphin.async.dnn.conf.LayerConfigurationParameters.*;
import edu.snu.cay.dolphin.async.dnn.layers.LayerBase;
import edu.snu.cay.dolphin.async.dnn.layers.LayerParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.bytedeco.javacpp.Pointer;

import javax.inject.Inject;

import static edu.snu.cay.dolphin.async.dnn.util.NeuralNetworkUtils.getShapeLength;

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
  private Matrix output;
  private Matrix layerError;

  private Pointer inputDesc;
  private Pointer activationDesc;

  private final int inputChannel;
  private final int inputHeight;
  private final int inputWidth;

  /**
   * @param index the index of this layer
   * @param inputShape the shape of input data
   * @param lossFunction the type of the loss function
   * @param activationFunction the type of the activation function
   * @param matrixFactory the factory to create new matrices
   */
  @Inject
  private ActivationWithLossGpuLayer(@Parameter(LayerIndex.class) final int index,
                                     @Parameter(LayerInputShape.class) final String inputShape,
                                     @Parameter(LossFunction.class) final String lossFunction,
                                     @Parameter(ActivationFunction.class) final String activationFunction,
                                     final MatrixFactory matrixFactory) {
    super(index, inputShape);
    this.lossFunction = lossFunction;
    this.matrixFactory = matrixFactory;
    this.output = null;
    this.layerError = null;

    if (getInputShape().length == 2) {
      this.inputChannel = 1;
      this.inputHeight = getInputShape()[0];
      this.inputWidth = getInputShape()[1];
    } else {
      this.inputChannel = getInputShape()[0];
      this.inputHeight = getInputShape()[1];
      this.inputWidth = getInputShape()[2];
    }

    //setup
    if (!activationFunction.toLowerCase().equals("softmax")) {
      throw new IllegalArgumentException("Unsupported activation function");
    }
    this.inputDesc = new Pointer();
    this.activationDesc = new Pointer();
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
    final int inputSize = input.getColumns();
    if (output == null || output.getColumns() != inputSize) {
      JavaCudnn.destroyTensorDesc(inputDesc);
      JavaCudnn.destroyTensorDesc(activationDesc);
      MatrixUtils.free(output);

      inputDesc = JavaCudnn.createTensorDesc(inputSize, inputChannel, inputHeight, inputWidth);
      activationDesc = JavaCudnn.createTensorDesc(inputSize, inputChannel, inputHeight, inputWidth);
      output = matrixFactory.create(getShapeLength(getOutputShape()), inputSize);
    }

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
      if (layerError == null || layerError.getColumns() != activation.getColumns()) {
        MatrixUtils.free(layerError);
        layerError = matrixFactory.create(activation.getRows(), activation.getColumns());
      }
      return activation.sub(label, layerError);
    default:
      throw new IllegalArgumentException("Unsupported loss function");
    }
  }

  @Override
  public LayerParameter generateParameterGradient(final Matrix input, final Matrix error) {
    throw new RuntimeException("This layer is not learnable");
  }

  @Override
  public void cleanup() {
    super.cleanup();
    JavaCudnn.destroyTensorDesc(inputDesc);
    JavaCudnn.destroyTensorDesc(activationDesc);

    MatrixUtils.free(output);
    MatrixUtils.free(layerError);
  }
}
