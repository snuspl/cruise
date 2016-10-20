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
 * Default parameter initializer.
 *
 * This initializer is for layers which do not have layer parameters (i.e. not learnable layer).
 * This initializer is used for layers whose output shape is equal to input shape.
 */
public final class DefaultLayerParameterInitializer implements LayerParameterInitializer {

  private final int index;
  private final LayerShape inputShape;
  private final LayerParameter emptyLayerParam;

  /**
   * @param index the index of this layer
   * @param inputShape the shape of input data
   */
  @Inject
  public DefaultLayerParameterInitializer(@Parameter(LayerIndex.class) final int index,
                                          @Parameter(LayerInputShape.class) final String inputShape) {
    this.index = index;
    this.inputShape = shapeFromString(inputShape);
    this.emptyLayerParam = LayerParameter.newEmptyInstance();
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

  @Override
  public LayerShape getOutputShape() {
    return inputShape;
  }

}
