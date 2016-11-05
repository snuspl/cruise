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

import static edu.snu.cay.dolphin.async.dnn.util.NeuralNetworkUtils.shapeFromString;

/**
 * Abstract class for the layer of a neural network.
 */
public abstract class LayerBase {

  private final int index;
  private LayerParameter layerParameter;
  private final LayerShape inputShape;

  protected LayerBase(final int index, final String inputShapeString) {
    this.index = index;
    this.inputShape = shapeFromString(inputShapeString);
  }

  /**
   * @return the index of the layer.
   */
  public final int getIndex() {
    return this.index;
  }

  /**
   * @return the shape of inputs.
   */
  public final LayerShape getInputShape() {
    return this.inputShape;
  }

  /**
   * @return the shape of outputs.
   */
  public abstract LayerShape getOutputShape();

  /**
   * Replaces the parameter of the layer.
   * @param layerParameter a new parameter of the layer.
   */
  public final void setLayerParameter(final LayerParameter layerParameter) {
    if (!isLearnable()) {
      throw new RuntimeException(this + " is not a learnable layer. setLayerParameter() should not be called.");
    }

    if (this.layerParameter != null) {
      this.layerParameter.cleanup();
    }
    this.layerParameter = layerParameter;
  }

  /**
   * @return the parameter of the layer.
   */
  public final LayerParameter getLayerParameter() {
    if (!isLearnable()) {
      throw new RuntimeException(this + " is not a learnable layer. getLayerParameter() should not be called.");
    }

    return this.layerParameter;
  }

  /**
   * @return whether this layer can learn from training data or not.
   */
  public abstract boolean isLearnable();

  /**
   * Computes output values.
   * @param input input values for this layer.
   * @return output values for this layer.
   */
  public abstract Matrix feedForward(Matrix input);

  /**
   * Computes errors.
   * @param input the input values for this layer, or the expected output if the layer is a loss layer.
   * @param activation the output values.
   * @param nextError the errors of the next layer - the one closer to the output layer.
   * @return errors for this layer with the specified input value.
   */
  public abstract Matrix backPropagate(Matrix input,
                                       Matrix activation,
                                       Matrix nextError);

  /**
   * Computes parameter gradients for this layer.
   * @param input inputs for this layer.
   * @param nextError the errors of the next layer - the one closer to the output layer.
   * @return the element-wise sum of parameter gradients for the specified input and error,
   *         or {@code null} if this layer is not learnable.
   */
  public abstract LayerParameter generateParameterGradient(Matrix input, Matrix nextError);

  /**
   * Clean up variables associated with this layer, this is called when this layer is no longer used.
   */
  public void cleanup() {
    if (layerParameter != null) {
      layerParameter.cleanup();
    }
  }
}
