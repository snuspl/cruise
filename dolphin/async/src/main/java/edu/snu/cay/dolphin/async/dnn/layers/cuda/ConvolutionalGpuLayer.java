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
 * Gpu convolutional layer.
 *
 * This layer is learnable having the updatable parameter (weight and bias).
 * This layer works for 2D and 3D inputs.
 * In a forward pass,
 * feedForward function computes the product between weight and the input within kernel range
 * and produce activation matrix.
 * In a backward pass,
 * the error of each input pixel comes from the product
 * between weight and errors of output pixels affected by the input pixel in feedforward step.
 */
public final class ConvolutionalGpuLayer extends LayerBase {

  private final int inputChannel;
  private final int inputHeight;
  private final int inputWidth;
  private final int kernelHeight;
  private final int kernelWidth;
  private final int[] outputShape;
  private MatrixFactory matrixFactory;

  private Pointer inputDesc;
  private Pointer activationDesc;
  private Pointer filterDesc;
  private Pointer convDesc;
  private Pointer biasDesc;

  private final Pointer forwardAlgo;
  private final Pointer backwardFilterAlgo;
  private final Pointer backwardDataAlgo;

  private final long forwardWorkspaceSize;
  private final long backwardFilterWorkspaceSize;
  private final long backwardDataWorkspaceSize;
  private long maxWorkspaceSize;
  private final Pointer workspace;

  /**
   * @param index the index of this layer
   * @param inputShape the shape of input data
   * @param paddingHeight the number of pixels to add to the top and bottom of the input images
   * @param paddingWidth the number of pixels to add to the left and right sides of the input images
   * @param strideHeight the vertical intervals at which to apply the filters to the input images
   * @param strideWidth the horizontal intervals at which to apply the filters to the input images
   * @param kernelHeight the height of the filters
   * @param kernelWidth the width of the filters
   * @param batchSize the batch size (number of images) this layer is receiving
   * @param layerParameterInitializer the layer parameter initializer that generates the new layer parameter following
   *                                  the configuration defined by users
   * @param matrixFactory the factory to create new matrices
   * @param batchSize the batch Size (number of images) of this layer
   */
  @Inject
  private ConvolutionalGpuLayer(@Parameter(LayerIndex.class) final int index,
                                @Parameter(LayerInputShape.class) final String inputShape,
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
    this.kernelHeight = kernelHeight;
    this.kernelWidth = kernelWidth;
    this.outputShape = layerParameterInitializer.getOutputShape();
    this.matrixFactory = matrixFactory;
    this.maxWorkspaceSize = 0;

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
    this.inputDesc = JavaCudnn.createTensorDesc(batchSize, inputChannel, inputHeight, inputWidth);
    detectErrorPointer(inputDesc);
    this.filterDesc = JavaCudnn.createFilterDesc(outputShape[0], inputChannel, kernelHeight, kernelWidth);
    detectErrorPointer(filterDesc);
    this.convDesc = JavaCudnn.createConvDesc(paddingHeight, paddingWidth, strideHeight, strideWidth);
    detectErrorPointer(convDesc);
    this.activationDesc = JavaCudnn.createTensorDesc(batchSize, outputShape[0], outputShape[1], outputShape[2]);
    detectErrorPointer(activationDesc);
    this.biasDesc = JavaCudnn.createTensorDesc(1, outputShape[0], 1, 1);
    detectErrorPointer(biasDesc);

    this.forwardAlgo = JavaCudnn.getConvForwardAlgo(inputDesc, filterDesc, convDesc, activationDesc);
    detectErrorPointer(forwardAlgo);
    this.backwardDataAlgo = JavaCudnn.getConvBackwardDataAlgo(filterDesc, activationDesc, convDesc, inputDesc);
    detectErrorPointer(backwardDataAlgo);
    this.backwardFilterAlgo = JavaCudnn.getConvBackwardFilterAlgo(inputDesc, activationDesc, convDesc, filterDesc);
    detectErrorPointer(backwardFilterAlgo);

    this.forwardWorkspaceSize = JavaCudnn.getConvForwardWorkspaceSizeInBytes(
        inputDesc, filterDesc, convDesc, activationDesc, forwardAlgo);
    this.backwardDataWorkspaceSize = JavaCudnn.getConvBackwardDataWorkspaceSizeInBytes(
        filterDesc, activationDesc, convDesc, inputDesc, backwardDataAlgo);
    this.backwardFilterWorkspaceSize = JavaCudnn.getConvBackwardFilterWorkspaceSizeInBytes(
        inputDesc, activationDesc, convDesc, filterDesc, backwardFilterAlgo);
    this.workspace = JavaCudnn.getWorkspace(maxWorkspaceSize);
    detectErrorPointer(workspace);
  }

  private void detectErrorPointer(final Pointer ptr) {
    if (ptr == null) {
      throw new RuntimeException("Null was passed for pointer");
    }
  }

  @Override
  public int[] getOutputShape() {
    return outputShape;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isLearnable() {
    return true;
  }

  /**
   * Computes output values for this convolutional layer.
   * @param input input values for this layer.
   * @return output values for this layer.
   */
  @Override
  public Matrix feedForward(final Matrix input) {
    final Matrix output = matrixFactory.create(NeuralNetworkUtils.getShapeLength(outputShape), input.getColumns());
    if (JavaCudnn.convFeedForward(inputDesc, ((MatrixCudaImpl) input).getDevicePointer(),
        filterDesc, ((MatrixCudaImpl) getLayerParameter().getWeightParam()).getDevicePointer(),
        biasDesc, ((MatrixCudaImpl) getLayerParameter().getBiasParam()).getDevicePointer(),
        convDesc, forwardAlgo, workspace, forwardWorkspaceSize,
        activationDesc, ((MatrixCudaImpl) output).getDevicePointer())) {
      return output;
    } else {
      throw new RuntimeException("Something went wrong in feedForward");
    }
  }

  /**
   * Computes errors for this convolutional layer.
   * @param input the input values for this layer.
   * @param activation the output values.
   * @param nextError the errors of the next layer - the one closer to the output layer.
   * @return errors for this layer with the specified input value.
   */
  @Override
  public Matrix backPropagate(final Matrix input, final Matrix activation, final Matrix nextError) {
    final Matrix error = matrixFactory.create(input.getRows(), input.getColumns());
    if (JavaCudnn.convBackPropagate(
        filterDesc, ((MatrixCudaImpl) getLayerParameter().getWeightParam()).getDevicePointer(),
        activationDesc, ((MatrixCudaImpl) nextError).getDevicePointer(), convDesc, backwardDataAlgo, workspace,
        backwardDataWorkspaceSize, inputDesc, ((MatrixCudaImpl) error).getDevicePointer())) {
      return error;
    } else {
      throw new RuntimeException("Something went wrong in backPropagate");
    }
  }

  /** {@inheritDoc} */
  @Override
  public LayerParameter generateParameterGradient(final Matrix input, final Matrix error) {
    final Matrix weightGradient = matrixFactory.create(kernelHeight * kernelWidth * inputChannel, outputShape[0]);
    if (JavaCudnn.convGenWeightGradient(inputDesc, ((MatrixCudaImpl) input).getDevicePointer(),
        activationDesc, ((MatrixCudaImpl) error).getDevicePointer(), convDesc, backwardFilterAlgo, workspace,
        backwardFilterWorkspaceSize, filterDesc, ((MatrixCudaImpl) weightGradient).getDevicePointer())) {
      final Matrix biasGradient = matrixFactory.create(outputShape[0], 1);
      if (JavaCudnn.convGenBiasGradient(activationDesc, ((MatrixCudaImpl) error).getDevicePointer(),
          biasDesc, ((MatrixCudaImpl) biasGradient).getDevicePointer())) {
        return LayerParameter.newBuilder()
            .setWeightParam(weightGradient)
            .setBiasParam(biasGradient)
            .build();
      } else {
        throw new RuntimeException("Something went wrong in generateParameterGradient for bias");
      }
    } else {
      throw new RuntimeException("Something went wrong in generateParameterGradient for weight");
    }
  }
}
