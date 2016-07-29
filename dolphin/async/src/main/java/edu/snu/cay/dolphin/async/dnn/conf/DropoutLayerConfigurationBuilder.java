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

import edu.snu.cay.dolphin.async.dnn.layers.DropoutLayer;
import edu.snu.cay.dolphin.async.dnn.layers.LayerBase;
import edu.snu.cay.dolphin.async.dnn.proto.NeuralNetworkProtos;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.util.Builder;

/**
 * Configuration builder for dropout layer.
 *
 * The configuration that this builder generates is used to create a dropout layer instance.
 */
public final class DropoutLayerConfigurationBuilder implements Builder<Configuration> {

  public static DropoutLayerConfigurationBuilder newConfigurationBuilder() {
    return new DropoutLayerConfigurationBuilder();
  }

  private float dropoutRatio = 0.5f;

  public synchronized DropoutLayerConfigurationBuilder setDropoutRatio(final float dropoutRatio) {
    this.dropoutRatio = dropoutRatio;
    return this;
  }

  public synchronized DropoutLayerConfigurationBuilder fromProtoConfiguration(
      final NeuralNetworkProtos.LayerConfiguration protoConf) {
    dropoutRatio = protoConf.getDropoutParam().getDropoutRatio();
    return this;
  }

  @Override
  public synchronized Configuration build() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(LayerConfigurationParameters.DropoutRatio.class, String.valueOf(dropoutRatio))
        .bindImplementation(LayerBase.class, DropoutLayer.class)
        .build();
  }
}
