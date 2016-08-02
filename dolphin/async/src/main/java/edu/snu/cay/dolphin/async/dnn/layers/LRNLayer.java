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
 *
 * This layer aids generalization done by activation layer
 */

public final class LRNLayer extends LayerBase {

  private final int localSize;
  private final float alpha;
  private final float beta;
  private final float k;
  private MatrixFactory matrixFactory;
  private int channelSize;
  private int padSize;
  private Matrix scale;

  /**
   * @param index the index of this layer
   * @param inputShape the shape of input data
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
    this.localSize = localSize; // always odd
    this.alpha = alpha;
    this.beta = beta;
    this.k = k;
    this.matrixFactory = matrixFactory;
    this.padSize = (localSize - 1) / 2;

    if (getInputShape().length == 3) {
      this.channelSize = getInputShape()[1] * getInputShape()[2];
    } else {
      this.channelSize = getInputShape()[0] * getInputShape()[1];
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
    scale = matrixFactory.create(input.getRows(), input.getColumns());
    final Matrix padded = MatrixFunctions.pow(padder(input), 2);
    scale = summer(scale, padded).mul(alpha / localSize).add(k);
    return input.mul(MatrixFunctions.pow(scale, -beta));
  }

  /**
   * Adds padding, size of ((localSize - 1) / 2) * channelSize on both ends of each image vector.
   * @param input input matrix to be padded
   * @return padded matrix
   */
  private Matrix padder(final Matrix input) {
    final float[] out = new float[(input.getRows() + ((localSize - 1) * channelSize)) * input.getColumns()];
    final float[] in = input.toFloatArray();
    for (int i = 0; i < in.length; ++i) {
      out[padSize + i] = in[i];
    }
    return matrixFactory.create(out, input.getRows() + ((localSize - 1) * channelSize), input.getColumns());
  }

  /**
   * Splits one image vector to a matrix so that each column represents one kernel.
   * @param vec image vector
   * @return split matrix
   */
  private Matrix splitter(final Matrix vec) {
    return vec.reshape(channelSize, vec.getLength() / channelSize);
  }

  private Matrix summer(final Matrix output, final Matrix padded) {
    // go through images
    for (int i = 0; i < output.getColumns(); ++i) {
      final Matrix errorI = matrixFactory.create(channelSize, scale.getRows() / channelSize);
      final Matrix paddedI = splitter(padded.getColumn(i));
      //first channel
      for (int l = 0; l < localSize; ++l) {
        errorI.putColumn(0, errorI.getColumn(0).add(paddedI.getColumn(l)));
      }
      //rest of the channels
      for (int c = 1; c < output.getRows() / channelSize; ++c) {
        errorI.putColumn(c, errorI.getColumn(c - 1).add(paddedI.getColumn(c + (padSize * 2)))
            .sub(paddedI.getColumn(c - 1)));
      }
      output.putColumn(i, errorI.reshape(scale.getRows(), 1));
    }
    return output;
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
    Matrix error = matrixFactory.create(input.getRows(), input.getColumns());
    final Matrix padded = padder(nextError.mul(activation).div(scale));
    error = summer(error, padded).mul(input).mul(-2 * alpha * beta / localSize);
    return error.add(MatrixFunctions.pow(scale, -beta).mul(nextError));
  }

  /** {@inheritDoc} */
  @Override
  public LayerParameter generateParameterGradient(final Matrix input, final Matrix error) {
    throw new RuntimeException("This layer is not learnable");
  }
}
