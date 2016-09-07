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
import edu.snu.cay.dolphin.async.dnn.layerparam.initializer.LayerParameterInitializer;
import edu.snu.cay.dolphin.async.dnn.layers.LayerBase;
import edu.snu.cay.dolphin.async.dnn.layers.LayerParameter;
import edu.snu.cay.dolphin.async.dnn.util.NeuralNetworkUtils;
import org.apache.reef.tang.annotations.Parameter;
import org.bytedeco.javacpp.Pointer;

import javax.inject.Inject;

/**
 * Gpu pooling layer.
 *
 * This layer is not learnable.
 * This layer resizes input matrix spatially, using max pooling or average pooling.
 * This layer works for 2D and 3D inputs.
 * We use cuDNN library to implement this layer.
 */
public final class PoolingGpuLayer extends LayerBase {

  private final int[] outputShape;
  private final MatrixFactory matrixFactory;
  private final char poolingType;

  private Pointer inputDesc;
  private Pointer activationDesc;
  private Pointer poolDesc;

  /**
   * @param index the index of this layer
   * @param inputShape the shape of input data
   * @param poolingType the pooling method
   * @param paddingHeight the number of pixels to add to the top and bottom of the input images
   * @param paddingWidth the number of pixels to add to the left and right sides of the input images
   * @param strideHeight the vertical intervals at which to apply the filters to the input images
   * @param strideWidth the horizontal intervals at which to apply the filters to the input images
   * @param kernelHeight the height of the filters
   * @param kernelWidth the width of the filters
   * @param batchSize the batch Size (number of images) of this layer
   * @param layerParameterInitializer the layer parameter initializer that generates the empty layer parameter
   * @param matrixFactory the factory to create new matrices
   */
  @Inject
  private PoolingGpuLayer(@Parameter(LayerIndex.class) final int index,
                          @Parameter(LayerInputShape.class) final String inputShape,
                          @Parameter(PoolingType.class) final String poolingType,
                          @Parameter(PaddingHeight.class) final int paddingHeight,
                          @Parameter(PaddingWidth.class) final int paddingWidth,
                          @Parameter(StrideHeight.class) final int strideHeight,
                          @Parameter(StrideWidth.class) final int strideWidth,
                          @Parameter(KernelHeight.class) final int kernelHeight,
                          @Parameter(KernelWidth.class) final int kernelWidth,
                          @Parameter(NeuralNetworkConfigurationParameters.BatchSize.class) final int batchSize,
                          final LayerParameterInitializer layerParameterInitializer,
                          final MatrixFactory matrixFactory) {
    super(index, inputShape);

    final int outputChannel;
    final int outputHeight;
    final int outputWidth;
    this.outputShape = layerParameterInitializer.getOutputShape();
    if (outputShape.length == 2) {
      outputChannel = 1;
      outputHeight = outputShape[0];
      outputWidth = outputShape[1];
    } else {
      outputChannel = outputShape[0];
      outputHeight = outputShape[1];
      outputWidth = outputShape[2];
    }

    final int inputHeight;
    final int inputWidth;
    final int inputChannel;
    if ((poolingType.toUpperCase()).equals("MAX")) {
      this.poolingType = 'M';
    } else {
      this.poolingType = 'A';
    }
    this.matrixFactory = matrixFactory;

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
    JavaCudnn.checkNullPointer(inputDesc);
    this.poolDesc = JavaCudnn.createPoolDesc(
        this.poolingType, kernelHeight, kernelWidth, paddingHeight, paddingWidth, strideHeight, strideWidth);
    JavaCudnn.checkNullPointer(poolDesc);
    this.activationDesc = JavaCudnn.createTensorDesc(batchSize, outputChannel, outputHeight, outputWidth);
    JavaCudnn.checkNullPointer(activationDesc);
  }

  @Override
  public int[] getOutputShape() {
    return outputShape;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isLearnable() {
    return false;
  }

  /**
   * Computes output values for this pooling layer.
   * available pooling type: max, average
   * @param input the input values for this layer.
   * @return the output values for this layer.
   */
  @Override
  public Matrix feedForward(final Matrix input) {
    final Matrix output = matrixFactory.create(NeuralNetworkUtils.getShapeLength(outputShape), input.getColumns());
    if (JavaCudnn.poolFeedForward(poolDesc, inputDesc, ((MatrixCudaImpl) input).getDevicePointer(),
        activationDesc, ((MatrixCudaImpl) output).getDevicePointer())) {
      return output;
    } else {
      throw new RuntimeException("Failed to feedForward");
    }
  }

  /**
   * Computes errors for this pooling layer.
   * available pooling type: max, average
   * @param input the input values for this layer.
   * @param activation the output values.
   * @param nextError the errors of the next layer - the one closer to the output layer.
   * @return errors for this layer with the specified input value.
   */
  @Override
  public Matrix backPropagate(final Matrix input, final Matrix activation, final Matrix nextError) {
    final Matrix error = matrixFactory.zeros(input.getRows(), input.getColumns());
    if (JavaCudnn.poolBackPropagate(poolDesc, activationDesc, ((MatrixCudaImpl) activation).getDevicePointer(),
        activationDesc, ((MatrixCudaImpl) nextError).getDevicePointer(), inputDesc,
        ((MatrixCudaImpl) input).getDevicePointer(), inputDesc, ((MatrixCudaImpl) error).getDevicePointer())) {
      return error;
    } else {
      throw new RuntimeException("Failed to backPropagate");
    }
  }

  /** {@inheritDoc} */
  @Override
  public LayerParameter generateParameterGradient(final Matrix input, final Matrix error) {
    throw new RuntimeException("This layer is not learnable");
  }
}
