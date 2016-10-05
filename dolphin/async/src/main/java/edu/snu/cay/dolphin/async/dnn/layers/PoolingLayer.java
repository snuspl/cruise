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
 * Pooling layer.
 *
 * This layer is not learnable.
 * This layer resizes input matrix spatially, using max pooling or average pooling.
 * This layer works for 2D and 3D inputs.
 * In a forward pass,
 * max pooling picks the maximum value in certain range (kernelHeight * kernelWidth) and these values make up output.
 * Average pooling gets the average of values in certain range (kernelHeight * kernelWidth)
 * and these values make up output.
 * In a backward pass,
 * error of each input pixel comes from errors of output pixels affected by the input pixel in feedforward step.
 */
public final class PoolingLayer extends LayerBase {

  private enum PoolType {
    AVERAGE, MAX
  }
  private final int[] outputShape;
  private final PoolType poolingType;
  private final int paddingHeight;
  private final int paddingWidth;
  private final int strideHeight;
  private final int strideWidth;
  private final int kernelHeight;
  private final int kernelWidth;
  private Matrix indexMatrix;
  private final int inputHeight;
  private final int inputWidth;
  private final int inputChannel;
  private final int outputHeight;
  private final int outputWidth;
  private final MatrixFactory matrixFactory;
  private Matrix output;
  private Matrix layerError;

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
   * @param layerParameterInitializer the layer parameter initializer that generates the empty layer parameter
   * @param matrixFactory the factory to create new matrices
   */
  @Inject
  private PoolingLayer(@Parameter(LayerIndex.class) final int index,
                       @Parameter(LayerInputShape.class) final String inputShape,
                       @Parameter(PoolingType.class) final String poolingType,
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
    this.poolingType = PoolType.valueOf(poolingType.toUpperCase());
    this.matrixFactory = matrixFactory;
    this.output = matrixFactory.create(0, 0);
    this.indexMatrix = matrixFactory.create(0, 0);
    this.layerError = matrixFactory.create(0, 0);
  
    if (getInputShape().length == 2) {
      this.inputChannel = 1;
      this.inputHeight = getInputShape()[0];
      this.inputWidth = getInputShape()[1];
      this.outputHeight = outputShape[0];
      this.outputWidth = outputHeight;
    } else {
      this.inputChannel = getInputShape()[0];
      this.inputHeight = getInputShape()[1];
      this.inputWidth = getInputShape()[2];
      this.outputHeight = outputShape[1];
      this.outputWidth = outputShape[2];
    }
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
   * Feedforward function for max pooling.
   * @param input the input values for this layer.
   * @return the output values for this layer.
   */
  private Matrix feedForwardMaxPooling(final Matrix input) {
    final int inputSize = inputHeight * inputWidth;
    final int outputSize = outputHeight * outputWidth;
    final int outputLength = NeuralNetworkUtils.getShapeLength(outputShape);
    if (output.getColumns() != input.getColumns()) {
      output = matrixFactory.create(outputLength, input.getColumns());
      indexMatrix = matrixFactory.create(outputLength, input.getColumns());
    }

    for (int n = 0; n < input.getColumns(); ++n) {
      for (int c = 0; c < inputChannel; ++c) {
        for (int oh = 0; oh < outputHeight; ++oh) {
          for (int ow = 0; ow < outputWidth; ++ow) {
            //Find the maximum value within kernel range and put it in the output matrix.
            int hstart = strideHeight * oh - paddingHeight;
            int wstart = strideWidth * ow - paddingWidth;
            final int hend = Math.min(kernelHeight + hstart, inputHeight);
            final int wend = Math.min(kernelWidth + wstart, inputWidth);
            hstart = Math.max(hstart, 0);
            wstart = Math.max(wstart, 0);
            int maxIndex = c * inputSize + hstart * inputWidth + wstart;
            float max = input.get(maxIndex, n);
            for (int kh = hstart; kh < hend; ++kh) {
              for (int kw = wstart; kw < wend; ++kw) {
                final int newIndex = c * inputSize + kh * inputWidth + kw;
                final float newValue = input.get(newIndex, n);
                if (newValue > max) {
                  max = newValue;
                  maxIndex = newIndex;
                }
              }
            }
            final int outputIndex = c * outputSize + oh * outputWidth + ow;
            output.put(outputIndex, n, max);
            //Save the index of max value.
            indexMatrix.put(outputIndex, n, maxIndex);
          }
        }
      }
    }
    return output;
  }

  /**
   * Feedforward function for average pooling.
   * @param input the input values for this layer.
   * @return the output values for this layer.
   */
  private Matrix feedForwardAveragePooling(final Matrix input) {
    final int inputSize = inputHeight * inputWidth;
    final int outputSize = outputHeight * outputWidth;
    if (output.getColumns() != input.getColumns()) {
      output = matrixFactory.create(NeuralNetworkUtils.getShapeLength(outputShape), input.getColumns());
    }

    for (int n = 0; n < input.getColumns(); ++n) {
      for (int c = 0; c < inputChannel; ++c) {
        for (int oh = 0; oh < outputHeight; ++oh) {
          for (int ow = 0; ow < outputWidth; ++ow) {
            //Compute sum of values within kernel range and put the average value in the output matrix.
            int hstart = strideHeight * oh - paddingHeight;
            int wstart = strideWidth * ow - paddingWidth;
            int hend = Math.min(kernelHeight + hstart, inputHeight + paddingHeight);
            int wend = Math.min(kernelWidth + wstart, inputWidth + paddingWidth);
            final int kernelSize = (hend - hstart) * (wend - wstart);
            hstart = Math.max(hstart, 0);
            wstart = Math.max(wstart, 0);
            hend = Math.min(hend, inputHeight);
            wend = Math.min(wend, inputWidth);
            float sum = 0;
            for (int kh = hstart; kh < hend; ++kh) {
              for (int kw = wstart; kw < wend; ++kw) {
                sum += input.get(c * inputSize + kh * inputWidth + kw, n);
              }
            }
            output.put(c * outputSize + oh * outputWidth + ow, n, sum / kernelSize);
          }
        }
      }
    }
    return output;
  }

  /**
   * Computes output values for this pooling layer.
   * available pooling type: max, average
   * @param input the input values for this layer.
   * @return the output values for this layer.
   */
  @Override
  public Matrix feedForward(final Matrix input) {
    switch (poolingType) {
    case MAX:
      return feedForwardMaxPooling(input);
    case AVERAGE:
      return feedForwardAveragePooling(input);
    default:
      throw new IllegalArgumentException("Illegal pooling type: " + poolingType);
    }
  }

  /**
   * Backpropagating function for max pooling.
   * @param input the input values for this layer.
   * @param nextError the errors of the next layer - the one closer to the output layer.
   * @return errors for this layer with the specified input value.
   */
  private Matrix backPropagateMaxPooling(final Matrix input, final Matrix nextError) {
    if (!layerError.hasSameSize(input)) {
      layerError = matrixFactory.create(input.getRows(), input.getColumns());
    }
    layerError.fill(0);

    final int outputSize = outputHeight * outputWidth;
    for (int n = 0; n < input.getColumns(); ++n) {
      for (int c = 0; c < inputChannel; ++c) {
        for (int oh = 0; oh < outputHeight; ++oh) {
          for (int ow = 0; ow < outputWidth; ++ow) {
            //Add error to saved index.
            final int outputIndex = c * outputSize + oh * outputWidth + ow;
            final int maxIndex = (int) indexMatrix.get(outputIndex, n);
            final float newError = nextError.get(outputIndex, n) + layerError.get(maxIndex, n);
            layerError.put(maxIndex, n, newError);
          }
        }
      }
    }
    return layerError;
  }

  /**
   * Backpropagating function for average pooling.
   * @param input the input values for this layer.
   * @param nextError the errors of the next layer - the one closer to the output layer.
   * @return errors for this layer with the specified input value.
   */
  private Matrix backPropagateAveragePooling(final Matrix input, final Matrix nextError) {
    if (!layerError.hasSameSize(input)) {
      layerError = matrixFactory.create(input.getRows(), input.getColumns());
    }
    layerError.fill(0);


    final int inputSize = inputHeight * inputWidth;
    final int outputSize = outputHeight * outputWidth;
    for (int n = 0; n < input.getColumns(); ++n) {
      for (int c = 0; c < inputChannel; ++c) {
        for (int oh = 0; oh < outputHeight; ++oh) {
          for (int ow = 0; ow < outputWidth; ++ow) {
            int hstart = strideHeight * oh - paddingHeight;
            int wstart = strideWidth * ow - paddingWidth;
            int hend = Math.min(kernelHeight + hstart, inputHeight + paddingHeight);
            int wend = Math.min(kernelWidth + wstart, inputWidth + paddingWidth);
            final int kernelSize = (hend - hstart) * (wend - wstart);
            hstart = Math.max(hstart, 0);
            wstart = Math.max(wstart, 0);
            hend = Math.min(hend, inputHeight);
            wend = Math.min(wend, inputWidth);
            final int outputIndex = c * outputSize + oh * outputWidth + ow;

            for (int kh = hstart; kh < hend; ++kh) {
              for (int kw = wstart; kw < wend; ++kw) {
                //Add error divided by kernel size for all pixels within the range.
                final int inputIndex = c * inputSize + kh * inputWidth + kw;
                final float newError = nextError.get(outputIndex, n) / kernelSize + layerError.get(inputIndex, n);
                layerError.put(inputIndex, n, newError);
              }
            }
          }
        }
      }
    }
    return layerError;
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
    switch (poolingType) {
    case MAX:
      return backPropagateMaxPooling(input, nextError);
    case AVERAGE:
      return backPropagateAveragePooling(input, nextError);
    default:
      throw new IllegalArgumentException("Illegal pooling type: " + poolingType);
    }
  }

  /** {@inheritDoc} */
  @Override
  public LayerParameter generateParameterGradient(final Matrix input, final Matrix error) {
    throw new RuntimeException("This layer is not learnable");
  }

  @Override
  public void cleanup() {

  }
}
