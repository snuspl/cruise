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
 * Local Response Normalization (LRN) layer.
 * This layer aids generalization done by activation layer.
 * The corresponding mathematical formula and explanation is at section 3.3 of the following paper.
 * https://papers.nips.cc/paper/4824-imagenet-classification-with-deep-convolutional-neural-networks.pdf
 */
public final class LRNGpuLayer extends LayerBase {

  private final int localSize;
  private final float alpha;
  private final float beta;
  private final float k;

  private final int inputChannel;
  private final int inputHeight;
  private final int inputWidth;
  private MatrixFactory matrixFactory;

  private Pointer inputDesc;
  private Pointer activationDesc;
  private Pointer lrnDesc;

  /**
   * @param index the index of this layer
   * @param inputShape the shape of input data
   * @param localSize the number of channels to sum over
   * @param alpha the scaling parameter
   * @param beta the exponent to raise the power of
   * @param k the constant to add
   * @param batchSize the batch Size (number of images) of this layer
   */
  @Inject
  private LRNGpuLayer(@Parameter(LayerIndex.class) final int index,
                      @Parameter(LayerInputShape.class) final String inputShape,
                      @Parameter(LocalSize.class) final int localSize,
                      @Parameter(Alpha.class) final float alpha,
                      @Parameter(Beta.class) final float beta,
                      @Parameter(K.class) final float k,
                      @Parameter(NeuralNetworkConfigurationParameters.BatchSize.class) final int batchSize,
                      final MatrixFactory matrixFactory) {
    super(index, inputShape);

    this.localSize = localSize;
    this.alpha = alpha;
    this.beta = beta;
    this.k = k;
    this.matrixFactory = matrixFactory;

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
    JavaCudnn.checkNullPointer(inputDesc);
    this.activationDesc = JavaCudnn.createTensorDesc(batchSize, inputChannel, inputHeight, inputWidth);
    JavaCudnn.checkNullPointer(activationDesc);
    this.lrnDesc = JavaCudnn.createLRNDesc(localSize, alpha, beta, k);
    JavaCudnn.checkNullPointer(lrnDesc);
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
   * @param input input values for this layer.
   * @return output values for this layer.
   */
  @Override
  public Matrix feedForward(final Matrix input) {
    final Matrix output = matrixFactory.create(input.getRows(), input.getColumns());
    if (JavaCudnn.lrnFeedForward(lrnDesc, inputDesc, ((MatrixCudaImpl) input).getDevicePointer(),
        activationDesc, ((MatrixCudaImpl) output).getDevicePointer())) {
      return output;
    } else {
      throw new RuntimeException("Failed to feedForward");
    }
  }

  /**
   * @param input the input values for this layer
   * @param activation the output values.
   * @param nextError the errors of the next layer - the one closer to the output layer.
   * @return errors for this layer with the specified input value.
   */
  @Override
  public Matrix backPropagate(final Matrix input,
                              final Matrix activation,
                              final Matrix nextError) {
    final Matrix error = matrixFactory.zeros(input.getRows(), input.getColumns());
    if (JavaCudnn.lrnBackPropagate(lrnDesc, activationDesc, ((MatrixCudaImpl) activation).getDevicePointer(),
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
