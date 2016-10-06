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

import edu.snu.cay.dolphin.async.dnn.conf.NeuralNetworkConfigurationParameters.SerializedLayerConfiguartion;
import edu.snu.cay.dolphin.async.dnn.layers.ActivationWithLossLayer;
import edu.snu.cay.dolphin.async.dnn.layers.LayerBase;
import edu.snu.cay.dolphin.async.dnn.layers.cuda.ActivationWithLossGpuLayer;
import edu.snu.cay.dolphin.async.dnn.proto.NeuralNetworkProtos;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.util.Builder;

/**
 * Configuration builder for activation with loss layer.
 *
 * The configuration that this builder generates is used to create an activation with loss layer instance.
 * The generated configuration needs to bind the parameter for a layer input shape, to inject a layer instance.
 */
public final class ActivationWithLossLayerConfigurationBuilder implements Builder<Configuration> {

  public static ActivationWithLossLayerConfigurationBuilder newConfigurationBuilder() {
    return new ActivationWithLossLayerConfigurationBuilder();
  }

  private String activationFunction;
  private String lossFunction;
  private Class<? extends LayerBase> layerClass = ActivationWithLossGpuLayer.class;

  private ConfigurationSerializer configurationSerializer = new AvroConfigurationSerializer();

  public synchronized ActivationWithLossLayerConfigurationBuilder setActivationFunction(
      final String activationFunction) {
    this.activationFunction = activationFunction;
    return this;
  }

  public synchronized ActivationWithLossLayerConfigurationBuilder setLossFunction(final String lossFunction) {
    this.lossFunction = lossFunction;
    return this;
  }

  public synchronized ActivationWithLossLayerConfigurationBuilder fromProtoConfiguration(
      final NeuralNetworkProtos.LayerConfiguration protoConf) {
    activationFunction = protoConf.getActivationWithLossParam().getActivationFunction();
    lossFunction = protoConf.getActivationWithLossParam().getLossFunction();
    return this;
  }

  public synchronized ActivationWithLossLayerConfigurationBuilder setCpuOnly(final boolean cpuOnly) {
    if (cpuOnly) {
      layerClass = ActivationWithLossLayer.class;
    } else {
      layerClass = ActivationWithLossGpuLayer.class;
    }
    return this;
  }

  @Override
  public synchronized Configuration build() {
    final JavaConfigurationBuilder configurationBuilder = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(LayerConfigurationParameters.LossFunction.class, lossFunction)
        .bindNamedParameter(LayerConfigurationParameters.ActivationFunction.class, String.valueOf(activationFunction))
        .bindImplementation(LayerBase.class, layerClass);

    if (layerClass == ActivationWithLossLayer.class) {
      final Configuration layerConf = ActivationLayerConfigurationBuilder.newConfigurationBuilder()
          .setActivationFunction(activationFunction)
          .setCpuOnly(true)
          .build();
      configurationBuilder
          .bindNamedParameter(SerializedLayerConfiguartion.class, configurationSerializer.toString(layerConf));
    }

    return configurationBuilder.build();
  }
}
