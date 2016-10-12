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
import edu.snu.cay.dolphin.async.dnn.conf.LayerConfigurationParameters.*;
import edu.snu.cay.dolphin.async.dnn.layerparam.initializer.LayerParameterInitializer;
import edu.snu.cay.dolphin.async.dnn.util.NeuralNetworkUtils;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * Convolutional layer.
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
public final class ConvolutionalLayer extends LayerBase {

  private final int[] outputShape;
  private final int paddingHeight;
  private final int paddingWidth;
  private final int strideHeight;
  private final int strideWidth;
  private final int kernelHeight;
  private final int kernelWidth;
  private final int inputHeight;
  private final int inputWidth;
  private final int inputChannel;
  private final MatrixFactory matrixFactory;

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
  private ConvolutionalLayer(@Parameter(LayerIndex.class) final int index,
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
    this.paddingHeight = paddingHeight;
    this.paddingWidth = paddingWidth;
    this.strideHeight = strideHeight;
    this.strideWidth = strideWidth;
    this.kernelHeight = kernelHeight;
    this.kernelWidth = kernelWidth;
    this.outputShape = layerParameterInitializer.getOutputShape();
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
   * Transform the given image to rows to facilitate matrix multiplication.
   * @param imageIndex the index of the image in the input matrix.
   * @param input input values for this layer.
   * @return the converted rows.
   */
  private Matrix im2row(final int imageIndex, final Matrix input) {
    final int inputSize = inputHeight * inputWidth;
    final int outputSize = outputShape[1] * outputShape[2];
    final Matrix row =
        matrixFactory.zeros(outputSize, kernelHeight * kernelWidth * inputChannel);
    for (int c = 0; c < inputChannel; ++c) {
      for (int kh = 0; kh < kernelHeight; ++kh) {
        for (int kw = 0; kw < kernelWidth; ++kw) {
          int ih = kh - paddingHeight;
          for (int oh = 0; oh < outputShape[1]; ++oh) {
            if (ih >= 0 && ih < inputHeight) {
              int iw = kw - paddingWidth;
              for (int ow = 0; ow < outputShape[2]; ++ow) {
                if (iw >= 0 && iw < inputWidth) {
                  row.put(oh * outputShape[2] + ow, c * kernelHeight * kernelWidth + kh * kernelWidth + kw,
                      input.get(c * inputSize + ih * inputWidth + iw, imageIndex));
                }
                iw += strideWidth;
              }
            }
            ih += strideHeight;
          }
        }
      }
    }
    return row;
  }

  /**
   * Transform the given rows to the image to facilitate matrix multiplication.
   * @param row the given rows.
   * @return the converted image.
   */
  private Matrix row2im(final Matrix row) {
    final int[] inputShape = getInputShape();
    final int inputSize = inputHeight * inputWidth;
    final Matrix im = matrixFactory.zeros(NeuralNetworkUtils.getShapeLength(inputShape));
    int rowIndex = 0;
    for (int c = 0; c < inputChannel; ++c) {
      for (int kh = 0; kh < kernelHeight; ++kh) {
        for (int kw = 0; kw < kernelWidth; ++kw) {
          int ih = kh - paddingHeight;
          for (int oh = 0; oh < outputShape[1]; ++oh) {
            if (ih < 0 || ih >= inputHeight) {
              rowIndex += outputShape[2];
            } else {
              int iw = kw - paddingWidth;
              for (int ow = 0; ow < outputShape[2]; ++ow) {
                if (iw >= 0 && iw < inputWidth) {
                  final int inputIndex = c * inputSize + ih * inputWidth + iw;
                  final float newValue = row.get(rowIndex) + im.get(inputIndex);
                  im.put(inputIndex, newValue);
                }
                rowIndex++;
                iw += strideWidth;
              }
            }
            ih += strideHeight;
          }
        }
      }
    }
    return im;
  }

  /**
   * Computes output values for this convolutional layer.
   * @param input input values for this layer.
   * @return output values for this layer.
   */
  @Override
  public Matrix feedForward(final Matrix input) {
    final Matrix output = matrixFactory.create(NeuralNetworkUtils.getShapeLength(outputShape), input.getColumns());
    for (int n = 0; n < input.getColumns(); ++n) {
      final Matrix newValue = im2row(n, input).mmul(getLayerParameter().getWeightParam());
      output.putColumn(n, newValue.reshape(NeuralNetworkUtils.getShapeLength(outputShape), 1));
    }
    output.addiColumnVector(getLayerParameter().getBiasParam());
    return output;
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
    for (int n = 0; n < input.getColumns(); ++n) {
      final Matrix singleNextError = nextError.getColumn(n).reshape(outputShape[1] * outputShape[2], outputShape[0]);
      final Matrix row = singleNextError.mmult(getLayerParameter().getWeightParam());
      final Matrix im = row2im(row);
      error.putColumn(n, im);
    }
    return error;
  }

  /** {@inheritDoc} */
  @Override
  public LayerParameter generateParameterGradient(final Matrix input, final Matrix error) {
    final Matrix weightGradient = matrixFactory.create(kernelHeight * kernelWidth * inputChannel, outputShape[0]);
    for (int n = 0; n < input.getColumns(); ++n) {
      final Matrix row = im2row(n, input);
      weightGradient.addi(row.tmmul(error.getColumn(n)
          .reshape(outputShape[1] * outputShape[2], outputShape[0])));
    }
    final Matrix biasGradient = error.rowSums();
    return new LayerParameter(weightGradient, biasGradient);
  }
}
