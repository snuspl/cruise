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
package edu.snu.cay.dolphin.async.dnn.conf;

//import edu.snu.cay.dolphin.async.dnn.layers.ActivationLayer;
import edu.snu.cay.dolphin.async.dnn.layers.LayerBase;
import edu.snu.cay.dolphin.async.dnn.layers.cuda.ActivationGpuLayer;
import edu.snu.cay.dolphin.async.dnn.proto.NeuralNetworkProtos;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.util.Builder;

/**
 * Configuration builder for activation layer.
 *
 * The configuration that this builder generates is used to create an activation layer instance.
 * The generated configuration needs to bind the parameter for a layer input shape, to inject a layer instance.
 */
public final class ActivationLayerConfigurationBuilder implements Builder<Configuration> {

  public static ActivationLayerConfigurationBuilder newConfigurationBuilder() {
    return new ActivationLayerConfigurationBuilder();
  }

  private String activationFunction;

  public synchronized ActivationLayerConfigurationBuilder setActivationFunction(final String activationFunction) {
    this.activationFunction = activationFunction;
    return this;
  }

  public synchronized ActivationLayerConfigurationBuilder fromProtoConfiguration(
      final NeuralNetworkProtos.LayerConfiguration protoConf) {
    activationFunction = protoConf.getActivationParam().getActivationFunction();
    return this;
  }

  @Override
  public synchronized Configuration build() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(LayerConfigurationParameters.ActivationFunction.class, String.valueOf(activationFunction))
        .bindImplementation(LayerBase.class, ActivationGpuLayer.class)
        .build();
  }
}
