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
import edu.snu.cay.dolphin.async.dnn.layers.LayerShape;
import org.apache.reef.tang.annotations.Parameter;
import org.bytedeco.javacpp.Pointer;

import javax.inject.Inject;

import static edu.snu.cay.dolphin.async.dnn.util.NeuralNetworkUtils.getShapeLength;

/**
 * Local Response Normalization (LRN) layer.
 * This layer aids generalization done by activation layer.
 * We use cuDNN library to implement this layer.
 */
public final class LRNGpuLayer extends LayerBase {

  private MatrixFactory matrixFactory;
  private Matrix output;
  private Matrix layerError;

  private Pointer inputDesc;
  private Pointer activationDesc;
  private final Pointer lrnDesc;

  private final int inputChannel;
  private final int inputHeight;
  private final int inputWidth;

  /**
   * @param index the index of this layer
   * @param inputShape the shape of input data
   * @param localSize the number of channels to sum over
   * @param alpha the scaling parameter
   * @param beta the exponent to raise the power of
   * @param k the constant to add
   */
  @Inject
  private LRNGpuLayer(@Parameter(LayerIndex.class) final int index,
                      @Parameter(LayerInputShape.class) final String inputShape,
                      @Parameter(LocalSize.class) final int localSize,
                      @Parameter(Alpha.class) final float alpha,
                      @Parameter(Beta.class) final float beta,
                      @Parameter(K.class) final float k,
                      final MatrixFactory matrixFactory) {
    super(index, inputShape);
    this.matrixFactory = matrixFactory;
    this.output = null;
    this.layerError = null;

    this.inputChannel = getInputShape().getChannel();
    this.inputHeight = getInputShape().getHeight();
    this.inputWidth = getInputShape().getWidth();

    //setup
    this.inputDesc = new Pointer();
    this.activationDesc = new Pointer();
    this.lrnDesc = JavaCudnn.createLRNDesc(localSize, alpha, beta, k);
  }

  @Override
  public LayerShape getOutputShape() {
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
    final int inputSize = input.getColumns();
    if (output == null || output.getColumns() != inputSize) {
      JavaCudnn.destroyTensorDesc(inputDesc);
      JavaCudnn.destroyTensorDesc(activationDesc);
      MatrixUtils.free(output);

      inputDesc = JavaCudnn.createTensorDesc(inputSize, inputChannel, inputHeight, inputWidth);
      activationDesc = JavaCudnn.createTensorDesc(inputSize, inputChannel, inputHeight, inputWidth);
      output = matrixFactory.create(getShapeLength(getOutputShape()), input.getColumns());
    }

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
    if (layerError == null || layerError.getColumns() != input.getColumns()) {
      MatrixUtils.free(layerError);
      layerError = matrixFactory.create(input.getRows(), input.getColumns());
    }
    layerError.fill(0);

    if (JavaCudnn.lrnBackPropagate(lrnDesc, activationDesc, ((MatrixCudaImpl) activation).getDevicePointer(),
        activationDesc, ((MatrixCudaImpl) nextError).getDevicePointer(), inputDesc,
        ((MatrixCudaImpl) input).getDevicePointer(), inputDesc, ((MatrixCudaImpl) layerError).getDevicePointer())) {
      return layerError;
    } else {
      throw new RuntimeException("Failed to backPropagate");
    }
  }

  /** {@inheritDoc} */
  @Override
  public LayerParameter generateParameterGradient(final Matrix input, final Matrix nextError) {
    throw new RuntimeException("This layer is not learnable");
  }

  @Override
  public void cleanup() {
    JavaCudnn.cudnnDestroyTensorDesc(inputDesc);
    JavaCudnn.cudnnDestroyTensorDesc(activationDesc);
    JavaCudnn.cudnnDestroyLRNDesc(lrnDesc);

    MatrixUtils.free(output);
    MatrixUtils.free(layerError);
  }
}
