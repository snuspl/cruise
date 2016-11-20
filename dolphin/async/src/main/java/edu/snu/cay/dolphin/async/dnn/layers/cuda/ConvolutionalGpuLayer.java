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

import edu.snu.cay.dolphin.async.dnn.blas.MatrixUtils;
import edu.snu.cay.dolphin.async.dnn.blas.cuda.JavaCuda;
import edu.snu.cay.dolphin.async.dnn.blas.Matrix;
import edu.snu.cay.dolphin.async.dnn.blas.MatrixFactory;
import edu.snu.cay.dolphin.async.dnn.blas.cuda.MatrixCudaImpl;
import edu.snu.cay.dolphin.async.dnn.conf.LayerConfigurationParameters.*;
import edu.snu.cay.dolphin.async.dnn.layerparam.initializer.LayerParameterInitializer;
import edu.snu.cay.dolphin.async.dnn.layers.LayerBase;
import edu.snu.cay.dolphin.async.dnn.layers.LayerParameter;
import edu.snu.cay.dolphin.async.dnn.layers.LayerShape;
import edu.snu.cay.dolphin.async.dnn.util.NeuralNetworkUtils;
import org.apache.reef.tang.annotations.Parameter;
import org.bytedeco.javacpp.Pointer;

import javax.inject.Inject;

/**
 * Gpu convolutional layer.
 *
 * This layer is learnable having the updatable parameter (weight and bias).
 * This layer works for 2D and 3D inputs.
 * We use cuDNN library to implement this layer.
 */
public final class ConvolutionalGpuLayer extends LayerBase {

  private final int inputChannel;
  private final int inputHeight;
  private final int inputWidth;
  private final int kernelWidth;
  private final int kernelHeight;
  private final LayerShape outputShape;
  private MatrixFactory matrixFactory;
  private Matrix output;
  private Matrix layerError;

  private final Pointer filterDesc;
  private final Pointer convDesc;
  private final Pointer biasDesc;

  private Pointer inputDesc;
  private Pointer activationDesc;
  private Pointer forwardAlgo;
  private Pointer backwardFilterAlgo;
  private Pointer backwardDataAlgo;

  private long forwardWorkspaceSize;
  private long backwardFilterWorkspaceSize;
  private long backwardDataWorkspaceSize;
  private long maxWorkspaceSize;
  private Pointer workspace;

  /**
   * @param index the index of this layer
   * @param inputShape the shape of input data
   * @param paddingHeight the number of pixels to add to the top and bottom of the input images
   * @param paddingWidth the number of pixels to add to the left and right sides of the input images
   * @param strideHeight the vertical intervals at which to apply the filters to the input images
   * @param strideWidth the horizontal intervals at which to apply the filters to the input images
   * @param kernelHeight the height of the filters
   * @param kernelWidth the width of the filters
   * @param layerParameterInitializer the layer parameter initializer that generates the new layer parameter following
   *                                  the configuration defined by users
   * @param matrixFactory the factory to create new matrices
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
                                final LayerParameterInitializer layerParameterInitializer,
                                final MatrixFactory matrixFactory) {
    super(index, inputShape);
    this.outputShape = layerParameterInitializer.getOutputShape();
    this.matrixFactory = matrixFactory;
    this.output = null;
    this.layerError = null;
    this.maxWorkspaceSize = 0;

    this.inputChannel = getInputShape().getChannel();
    this.inputHeight = getInputShape().getHeight();
    this.inputWidth = getInputShape().getWidth();
    this.kernelHeight = kernelHeight;
    this.kernelWidth = kernelWidth;

    final int outputChannel = outputShape.getChannel();

    //setup
    this.filterDesc = JavaCudnn.createFilterDesc(outputChannel, inputChannel, kernelHeight, kernelWidth);
    this.convDesc = JavaCudnn.createConvDesc(paddingHeight, paddingWidth, strideHeight, strideWidth);
    this.biasDesc = JavaCudnn.createTensorDesc(1, outputChannel, 1, 1);

    this.inputDesc = new Pointer();
    this.activationDesc = new Pointer();
    this.forwardAlgo = new Pointer();
    this.backwardDataAlgo = new Pointer();
    this.backwardFilterAlgo = new Pointer();

    this.forwardWorkspaceSize = 0;
    this.backwardDataWorkspaceSize = 0;
    this.backwardFilterWorkspaceSize = 0;
    this.maxWorkspaceSize = 0;
    this.workspace = null;
  }

  private void setMaxWorkspaceSize(final long workspaceSize) {
    if (workspaceSize > maxWorkspaceSize) {
      maxWorkspaceSize = workspaceSize;
    }
  }

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
   * Computes output values for this convolutional layer.
   * @param input input values for this layer.
   * @return output values for this layer.
   */
  @Override
  public Matrix feedForward(final Matrix input) {
    final int inputSize = input.getColumns();
    if (output == null || output.getColumns() != inputSize) {
      JavaCudnn.destroyTensorDesc(inputDesc);
      JavaCudnn.destroyTensorDesc(activationDesc);
      JavaCudnn.destroyAlgo(forwardAlgo);
      JavaCudnn.destroyAlgo(backwardDataAlgo);
      JavaCudnn.destroyAlgo(backwardFilterAlgo);
      JavaCuda.deviceFree(workspace);
      MatrixUtils.free(output);

      inputDesc = JavaCudnn.createTensorDesc(inputSize, inputChannel, inputHeight, inputWidth);
      activationDesc = JavaCudnn.createTensorDesc(inputSize,
          outputShape.getChannel(), outputShape.getHeight(), outputShape.getWidth());
      forwardAlgo = JavaCudnn.getConvForwardAlgo(inputDesc, filterDesc, convDesc, activationDesc);
      backwardDataAlgo = JavaCudnn.getConvBackwardDataAlgo(filterDesc, activationDesc, convDesc, inputDesc);
      backwardFilterAlgo = JavaCudnn.getConvBackwardFilterAlgo(inputDesc, activationDesc, convDesc, filterDesc);

      forwardWorkspaceSize = JavaCudnn.getConvForwardWorkspaceSizeInBytes(
          inputDesc, filterDesc, convDesc, activationDesc, forwardAlgo);
      setMaxWorkspaceSize(forwardWorkspaceSize);
      backwardDataWorkspaceSize = JavaCudnn.getConvBackwardDataWorkspaceSizeInBytes(
          filterDesc, activationDesc, convDesc, inputDesc, backwardDataAlgo);
      setMaxWorkspaceSize(backwardDataWorkspaceSize);
      backwardFilterWorkspaceSize = JavaCudnn.getConvBackwardFilterWorkspaceSizeInBytes(
          inputDesc, activationDesc, convDesc, filterDesc, backwardFilterAlgo);
      setMaxWorkspaceSize(backwardFilterWorkspaceSize);
      workspace = JavaCuda.deviceMalloc(maxWorkspaceSize);

      output = matrixFactory.create(NeuralNetworkUtils.getShapeLength(outputShape), inputSize);
    }

    if (JavaCudnn.convFeedForward(inputDesc, ((MatrixCudaImpl) input).getDevicePointer(),
        filterDesc, ((MatrixCudaImpl) getLayerParameter().getWeightParam()).getDevicePointer(),
        biasDesc, ((MatrixCudaImpl) getLayerParameter().getBiasParam()).getDevicePointer(),
        convDesc, forwardAlgo, workspace, forwardWorkspaceSize,
        activationDesc, ((MatrixCudaImpl) output).getDevicePointer())) {
      return output;
    } else {
      throw new RuntimeException("Failed to feedForward");
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
    if (layerError == null || layerError.getColumns() != input.getColumns()) {
      MatrixUtils.free(layerError);
      layerError = matrixFactory.create(input.getRows(), input.getColumns());
    }

    if (JavaCudnn.convBackPropagate(
        filterDesc, ((MatrixCudaImpl) getLayerParameter().getWeightParam()).getDevicePointer(),
        activationDesc, ((MatrixCudaImpl) nextError).getDevicePointer(), convDesc, backwardDataAlgo, workspace,
        backwardDataWorkspaceSize, inputDesc, ((MatrixCudaImpl) layerError).getDevicePointer())) {
      return layerError;
    } else {
      throw new RuntimeException("Failed to backPropagate");
    }
  }

  /** {@inheritDoc} */
  @Override
  public LayerParameter generateParameterGradient(final Matrix input, final Matrix nextError) {
    final Matrix weightGradient =
        matrixFactory.create(kernelHeight * kernelWidth * inputChannel, outputShape.getChannel());
    final Matrix biasGradient = matrixFactory.create(1, outputShape.getChannel());
    final LayerParameter parameterGradient = new LayerParameter(weightGradient, biasGradient);
    if (!JavaCudnn.convGenWeightGradient(inputDesc, ((MatrixCudaImpl) input).getDevicePointer(),
        activationDesc, ((MatrixCudaImpl) nextError).getDevicePointer(), convDesc, backwardFilterAlgo, workspace,
        backwardFilterWorkspaceSize, filterDesc,
        ((MatrixCudaImpl) parameterGradient.getWeightParam()).getDevicePointer())) {
      throw new RuntimeException("Failed to generateParameterGradient for weight");
    }
    if (JavaCudnn.convGenBiasGradient(activationDesc, ((MatrixCudaImpl) nextError).getDevicePointer(),
        biasDesc, ((MatrixCudaImpl) parameterGradient.getBiasParam()).getDevicePointer())) {
      return parameterGradient;
    } else {
      throw new RuntimeException("Failed to generateParameterGradient for bias");
    }
  }

  @Override
  public void cleanup() {
    super.cleanup();

    JavaCudnn.destroyTensorDesc(inputDesc);
    JavaCudnn.destroyFilterDesc(filterDesc);
    JavaCudnn.destroyConvDesc(convDesc);
    JavaCudnn.destroyTensorDesc(activationDesc);
    JavaCudnn.destroyTensorDesc(biasDesc);

    JavaCudnn.destroyAlgo(forwardAlgo);
    JavaCudnn.destroyAlgo(backwardDataAlgo);
    JavaCudnn.destroyAlgo(backwardFilterAlgo);
    JavaCuda.deviceFree(workspace);

    MatrixUtils.free(output);
    MatrixUtils.free(layerError);
  }
}
