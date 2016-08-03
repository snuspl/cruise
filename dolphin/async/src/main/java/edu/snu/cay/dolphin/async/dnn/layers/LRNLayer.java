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
package edu.snu.cay.dolphin.async.dnn.layers;

import edu.snu.cay.dolphin.async.dnn.blas.Matrix;
import edu.snu.cay.dolphin.async.dnn.blas.MatrixFactory;
import edu.snu.cay.dolphin.async.dnn.blas.MatrixFunctions;
import edu.snu.cay.dolphin.async.dnn.conf.LayerConfigurationParameters.*;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * Local Response Normalization (LRN) layer.
 * This layer aids generalization done by activation layer.
 * The corresponding mathematical formula and explanation is at section 3.3 of the following paper.
 * https://papers.nips.cc/paper/4824-imagenet-classification-with-deep-convolutional-neural-networks.pdf
 */

public final class LRNLayer extends LayerBase {

  private final int localSize;
  private final float alpha;
  private final float beta;
  private final float k;

  private final int inputSize;
  private final int inputChannel;
  private final int paddingSize;

  private final MatrixFactory matrixFactory;
  private Matrix scale;

  /**
   * @param index the index of this layer
   * @param inputShape the shape of input data
   * @param localSize the number of channels to sum over
   * @param alpha the scaling parameter
   * @param beta the exponent
   * @param k the constant
   * @param matrixFactory the factory to create new matrices
   */
  @Inject
  private LRNLayer(@Parameter(LayerIndex.class) final int index,
                   @Parameter(LayerInputShape.class) final String inputShape,
                   @Parameter(LocalSize.class) final int localSize,
                   @Parameter(Alpha.class) final float alpha,
                   @Parameter(Beta.class) final float beta,
                   @Parameter(K.class) final float k,
                   final MatrixFactory matrixFactory) {
    super(index, inputShape);

    this.localSize = localSize;

    this.alpha = alpha;
    this.beta = beta;
    this.k = k;
    this.paddingSize = (localSize - 1) / 2;
    this.matrixFactory = matrixFactory;

    if (getInputShape().length == 2) {
      this.inputChannel = 1;
      this.inputSize = getInputShape()[0] * getInputShape()[1];
    } else {
      this.inputChannel = getInputShape()[0];
      this.inputSize = getInputShape()[1] * getInputShape()[2];
    }
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
   * Computes output values for this dropout layer.
   * @param input input values for this layer.
   * @return output values for this layer.
   */
  @Override
  public Matrix feedForward(final Matrix input) {
    this.scale = matrixFactory.create(input.getRows(), input.getColumns());

    for (int n = 0; n < input.getColumns(); n++) {
      final Matrix paddedImg = matrixFactory.zeros(input.getRows() + (paddingSize * 2 * inputSize), 1);
      for (int i = 0; i < input.getRows(); i++) {
        paddedImg.put(i + paddingSize * inputSize, input.get(i, n) * input.get(i, n));
      }
      sum(scale, paddedImg, n);
    }
    scale.muli(alpha / localSize).addi(k);
    return MatrixFunctions.pow(scale, -beta).muli(input);
  }

  private void sum(final Matrix output, final Matrix padded, final int n) {
    final Matrix outputI = matrixFactory.create(inputSize, inputChannel);
    final Matrix paddedI = padded.reshape(inputSize, inputChannel + paddingSize * 2);
    //first channel
    for (int r = 0; r < outputI.getRows(); ++r) {
      float sum = 0F;
      for (int l = 0; l < localSize; l++) {
        sum += paddedI.get(r, l);
      }
      outputI.put(r, 0, sum);
    }
    //rest of the channels
    for (int c = 1; c < inputChannel; ++c) {
      for (int r = 0; r < outputI.getRows(); ++r) {
        outputI.put(r, c, outputI.get(r, c - 1) + paddedI.get(r, c + (paddingSize * 2)) - paddedI.get(r, c - 1));
//        outputI.putColumn(c, outputI.getColumn(c - 1).add(paddedI.getColumn(c + (paddingSize * 2)))
//                                                     .sub(paddedI.getColumn(c - 1)));
      }
    }
    output.putColumn(n, outputI.reshape(output.getRows(), 1));
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
    final Matrix error = matrixFactory.create(input.getRows(), input.getColumns());

    for (int n = 0; n < nextError.getColumns(); n++) {
      final Matrix paddedImg = matrixFactory.zeros(input.getRows() + (paddingSize * 2 * inputSize), 1);
      for (int i = 0; i < nextError.getRows(); i++) {
      // nextError * activation / scale
      // addPadding(nextError.mul(activation).div(scale));
        paddedImg.put(i + paddingSize * inputSize, nextError.get(i, n) * activation.get(i, n) / scale.get(i, n));
      }
      sum(error, paddedImg, n);
    }

    error.muli(input).muli(-2 * alpha * beta / localSize);
    return error.addi(MatrixFunctions.powi(scale, -beta).muli(nextError));
  }

  /** {@inheritDoc} */
  @Override
  public LayerParameter generateParameterGradient(final Matrix input, final Matrix error) {
    throw new RuntimeException("This layer is not learnable");
  }
}
