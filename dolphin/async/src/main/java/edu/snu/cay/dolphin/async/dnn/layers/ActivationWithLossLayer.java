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
import edu.snu.cay.dolphin.async.dnn.conf.LayerConfigurationParameters.*;
import edu.snu.cay.dolphin.async.dnn.conf.NeuralNetworkConfigurationParameters.SerializedLayerConfiguartion;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.io.IOException;

/**
 * Loss layer with activation function.
 *
 * This layer is intended to be the last layer, in most cases.
 * This layer is not learnable.
 * <br/>
 * In a forward pass,
 * this layer applies the specified activation function to each element of an input (same to {@link ActivationLayer}).
 * In a backward pass,
 * this layer computes the derivative of the specified loss function.
 */
public final class ActivationWithLossLayer extends LayerBase {

  private final LayerBase activationLayer;
  private final String lossFunction;

  private Matrix layerError;

  /**
   * @param index the index of this layer
   * @param inputShape the shape of input data
   * @param serializedLayerConf the serialized Tang configuration to inject the inner activation layer
   * @param configurationSerializer the serializer to deserialize Tang configurations for layer parameter initializer
   * @param lossFunction the type of the loss function
   */
  @Inject
  private ActivationWithLossLayer(@Parameter(LayerIndex.class) final int index,
                                  @Parameter(LayerInputShape.class) final String inputShape,
                                  @Parameter(SerializedLayerConfiguartion.class) final String serializedLayerConf,
                                  final ConfigurationSerializer configurationSerializer,
                                  @Parameter(LossFunction.class) final String lossFunction) {
    super(index, inputShape);
    this.lossFunction = lossFunction;
    this.layerError = null;

    try {
      // bind the layer index and input shape for injecting inner layer.
      final Configuration layerConf = Tang.Factory.getTang().newConfigurationBuilder(
          configurationSerializer.fromString(serializedLayerConf))
          .bindNamedParameter(LayerIndex.class, String.valueOf(index))
          .bindNamedParameter(LayerInputShape.class, inputShape)
          .build();
      this.activationLayer = Tang.Factory.getTang().newInjector(layerConf).getInstance(LayerBase.class);
    } catch (final IOException ioException) {
      throw new RuntimeException("IOException while de-serializing a layer configuration: " + ioException);
    } catch (final InjectionException injectException) {
      throw new RuntimeException("InjectionException while injecting activation layer: " + injectException);
    }
  }

  @Override
  public int[] getOutputShape() {
    return activationLayer.getOutputShape();
  }

  @Override
  public boolean isLearnable() {
    return false;
  }

  @Override
  public Matrix feedForward(final Matrix input) {
    return activationLayer.feedForward(input);
  }

  /**
   * Compute the error for the specified loss function.
   * @param label the label value.
   * @param activation the activation value.
   * @param nextError an error of the next layer - this argument is ignored.
   * @return the error with respect to the activation and label values.
   */
  @Override
  public Matrix backPropagate(final Matrix label, final Matrix activation, final Matrix nextError) {
    switch (lossFunction.toLowerCase()) {
    case "crossentropy":
      if (layerError == null) {
        layerError = activation.dup();
      } else {
        layerError.copy(activation);
      }
      return layerError.subi(label);
    default:
      throw new IllegalArgumentException("Unsupported loss function");
    }
  }

  @Override
  public LayerParameter generateParameterGradient(final Matrix input, final Matrix error) {
    throw new RuntimeException("This layer is not learnable");
  }

  @Override
  public void cleanup() {

  }
}
