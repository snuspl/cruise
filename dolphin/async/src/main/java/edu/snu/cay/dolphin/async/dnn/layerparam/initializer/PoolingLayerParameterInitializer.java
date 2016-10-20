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
package edu.snu.cay.dolphin.async.dnn.layerparam.initializer;

import edu.snu.cay.dolphin.async.dnn.conf.LayerConfigurationParameters.*;
import edu.snu.cay.dolphin.async.dnn.layers.LayerParameter;
import edu.snu.cay.dolphin.async.dnn.layers.LayerShape;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

import static edu.snu.cay.dolphin.async.dnn.util.NeuralNetworkUtils.shapeFromString;

/**
 * Pooling Layer parameter initializer.
 *
 * This initializer is for pooling layers which do not have layer parameters.
 */
public final class PoolingLayerParameterInitializer implements LayerParameterInitializer {

  private final int index;
  private final LayerShape inputShape;
  private final LayerShape outputShape;
  private final int paddingHeight;
  private final int paddingWidth;
  private final int strideHeight;
  private final int strideWidth;
  private final int kernelHeight;
  private final int kernelWidth;
  private final int inputHeight;
  private final int inputWidth;
  private final int inputChannel;
  private final LayerParameter emptyLayerParam;

  /**
   * @param index the index of this layer
   * @param inputShape the shape of input data
   * @param paddingHeight the number of pixels to add to the top and bottom of the input images
   * @param paddingWidth the number of pixels to add to the left and right sides of the input images
   * @param strideHeight the vertical intervals at which to apply the filters to the input images
   * @param strideWidth the horizontal intervals at which to apply the filters to the input images
   * @param kernelHeight the height of the filters
   * @param kernelWidth the width of the filters
   */
  @Inject
  private PoolingLayerParameterInitializer(@Parameter(LayerIndex.class) final int index,
                                           @Parameter(LayerInputShape.class) final String inputShape,
                                           @Parameter(PaddingHeight.class) final int paddingHeight,
                                           @Parameter(PaddingWidth.class) final int paddingWidth,
                                           @Parameter(StrideHeight.class) final int strideHeight,
                                           @Parameter(StrideWidth.class) final int strideWidth,
                                           @Parameter(KernelHeight.class) final int kernelHeight,
                                           @Parameter(KernelWidth.class) final int kernelWidth) {
    this.index = index;
    this.inputShape = shapeFromString(inputShape);
    this.paddingHeight = paddingHeight;
    this.paddingWidth = paddingWidth;
    this.strideHeight = strideHeight;
    this.strideWidth = strideWidth;
    this.kernelHeight = kernelHeight;
    this.kernelWidth = kernelWidth;
    this.emptyLayerParam = LayerParameter.newEmptyInstance();

    this.inputChannel = this.inputShape.getChannel();
    this.inputHeight = this.inputShape.getHeight();
    this.inputWidth = this.inputShape.getWidth();

    this.outputShape = computeOutputShape();
  }

  /**
   * @return the initial parameter of the layer.
   */
  public LayerParameter generateInitialParameter() {
    return emptyLayerParam;
  }

  /**
   * @return the index of the layer.
   */
  public int getIndex() {
    return index;
  }

  /**
   * This function computes output shape.
   * input shape: row * col
   * output shape: row' * col'
   * row' = ceil((row − kernelHeight + 2 * paddingHeight) / strideHeight) + 1
   * col' = ceil((col − kernelWidth + 2 * paddingWidth) / strideWidth) + 1
   * @return shape of output
   */
  private LayerShape computeOutputShape() {
    if (paddingHeight >= kernelHeight) {
      throw new IllegalArgumentException("Padding height should be less than kernel height.");
    }
    if (paddingWidth >= kernelWidth) {
      throw new IllegalArgumentException("Padding width should be less than kernel width.");
    }

    int outputHeight = (int) Math.ceil((float) (inputHeight - kernelHeight + 2 * paddingHeight) / strideHeight) + 1;
    int outputWidth = (int) Math.ceil((float) (inputWidth - kernelWidth + 2 * paddingWidth) / strideWidth) + 1;
    //Pooling should start inside the input images.
    //If the last pooling starts outside the input image, clip that output.
    if ((outputHeight - 1) * strideHeight >= inputHeight + paddingHeight) {
      --outputHeight;
      if ((outputHeight - 1) * strideHeight >= inputHeight + paddingHeight) {
        throw new IllegalArgumentException("The second last pooling still starts outside of the image " +
            "even though we clip the last.");
      }
    }
    if ((outputWidth - 1) * strideWidth >= inputWidth + paddingWidth) {
      --outputWidth;
      if ((outputWidth - 1) * strideWidth >= inputWidth + paddingWidth) {
        throw new IllegalArgumentException("The second last pooling still starts outside of the image " +
            "even though we clip the last.");
      }
    }
    if (outputHeight < 0 || outputWidth < 0) {
      throw new IllegalArgumentException("Negative output size");
    }

    return new LayerShape(inputChannel, outputHeight, outputWidth);
  }

  /**
   * @return shape of output
   */
  @Override
  public LayerShape getOutputShape() {
    return outputShape;
  }
}
