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
package edu.snu.cay.dolphin.async.dnn;

import edu.snu.cay.dolphin.async.dnn.conf.NeuralNetworkConfigurationParameters.*;
import edu.snu.cay.dolphin.async.dnn.layers.LayerParameter;
import edu.snu.cay.services.ps.server.api.ParameterUpdater;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.util.Set;

import static edu.snu.cay.dolphin.async.dnn.util.NeuralNetworkUtils.deserializeLayerConfSetToArray;
import static edu.snu.cay.dolphin.async.dnn.util.NeuralNetworkUtils.getInitialLayerParameters;

/**
 * This {@link ParameterUpdater} implementation depicts how the parameter server for the neural network module should
 * process data received from workers.
 * This class aggregates gradients according to batch size and subtracts them from the current network parameters
 */
public final class NeuralNetworkParameterUpdater implements ParameterUpdater<Integer, LayerParameter, LayerParameter> {

  private final float stepSize;
  private final LayerParameter[] initialLayerParameters;

  @Inject
  private NeuralNetworkParameterUpdater(
      @Parameter(SerializedLayerConfigurationSet.class) final Set<String> serializedLayerConfigurationSet,
      @Parameter(StepSize.class) final float stepSize,
      final ConfigurationSerializer configurationSerializer,
      @Parameter(InputShape.class) final String inputShape,
      final Injector injector) {
    final Configuration[] layerInitializerConfigurations =
        deserializeLayerConfSetToArray(configurationSerializer, serializedLayerConfigurationSet);
    this.stepSize = stepSize;
    this.initialLayerParameters = getInitialLayerParameters(injector, layerInitializerConfigurations, inputShape);
  }

  /**
   * Process a {@link LayerParameter} value given from a worker into server-friendly format.
   * Multiply the step size (in-place update).
   */
  @Override
  public LayerParameter process(final Integer key, final LayerParameter parameterGradient) {
    parameterGradient.getWeightParam().muli(stepSize);
    parameterGradient.getBiasParam().muli(stepSize);
    return parameterGradient;
  }

  /**
   * Update a {@link LayerParameter} value stored in the server using a value given from a worker.
   * @param layerParameter a layer parameter value stored in the server
   * @param parameterGradient a parameter gradient received from workers
   * @return the updated layer parameter
   */
  @Override
  public LayerParameter update(final LayerParameter layerParameter, final LayerParameter parameterGradient) {
    // Subtract the parameter gradient from the current layer parameter values (in-place update)
    layerParameter.getWeightParam().subi(parameterGradient.getWeightParam());
    layerParameter.getBiasParam().subi(parameterGradient.getBiasParam());
    return layerParameter;
  }

  /**
   * Get an initial value of {@link LayerParameter} specified by the user-defined configuration.
   * @param key the index of a layer
   * @return an initial layer parameter.
   */
  @Override
  public LayerParameter initValue(final Integer key) {
    if (key < 0 || key >= initialLayerParameters.length) {
      throw new RuntimeException("Invalid layer index: " + key);
    }
    return initialLayerParameters[key];
  }
}
