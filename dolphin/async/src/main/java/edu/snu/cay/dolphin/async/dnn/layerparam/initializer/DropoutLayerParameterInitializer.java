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

import edu.snu.cay.dolphin.async.dnn.blas.Matrix;
import edu.snu.cay.dolphin.async.dnn.blas.MatrixFactory;
import edu.snu.cay.dolphin.async.dnn.conf.LayerConfigurationParameters.*;
import edu.snu.cay.dolphin.async.dnn.layers.LayerParameter;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

import static edu.snu.cay.dolphin.async.dnn.util.NeuralNetworkUtils.shapeFromString;

/**
 * Dropout Layer parameter initializer.
 *
 * This initializer is for dropout layers which do not have layer parameters.
 */
public class DropoutLayerParameterInitializer implements LayerParameterInitializer {

  private final MatrixFactory matrixFactory;
  private final int index;
  private final int[] inputShape;
  private final int[] outputShape;
  private final float dropoutRatio;
  final LayerParameter emptyLayerParam;
  private Matrix bernoulliMatrix;
  private final int inputHeight;
  private final int inputWidth;
  private final int inputChannel;

  @Inject
  public DropoutLayerParameterInitializer(final MatrixFactory matrixFactory,
                                          @Parameter(LayerIndex.class) final int index,
                                          @Parameter(LayerInputShape.class) final String inputShape,
                                          @Parameter(DropoutRatio.class) final float dropoutRatio) {
    this.matrixFactory = matrixFactory;
    this.index = index;
    this.inputShape = shapeFromString(inputShape);
    this.outputShape = this.inputShape;
    this.dropoutRatio = dropoutRatio;
    this.emptyLayerParam = LayerParameter.newEmptyInstance(matrixFactory);

    if (this.inputShape.length == 2) {
      this.inputChannel = 1;
      this.inputHeight = this.inputShape[0];
      this.inputWidth = this.inputShape[1];
    } else {
      this.inputChannel = this.inputShape[0];
      this.inputHeight = this.inputShape[1];
      this.inputWidth = this.inputShape[2];
    }
    this.bernoulliMatrix = matrixFactory.bernoulli(this.inputHeight, this.inputWidth, dropoutRatio);
  }

  /**
   * @return the initial parameter of the layer.
   */
  public LayerParameter generateInitialParameter() {
    return emptyLayerParam;
  }

  /**
   * @return the dropout ratio of the layer
   */
  public float getDropoutRatio() {
    return this.dropoutRatio;
  }

  /**
   * @return the index of the layer.
   */
  @Override
  public int getIndex() {
    return this.index;
  }

  /**
   * @return shape of output
   */
  @Override
  public int[] getOutputShape() {
    return outputShape;
  }
}
