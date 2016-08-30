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

//import edu.snu.cay.dolphin.async.dnn.layers.LRNLayer;
import edu.snu.cay.dolphin.async.dnn.layers.LayerBase;
import edu.snu.cay.dolphin.async.dnn.layers.cuda.LRNGpuLayer;
import edu.snu.cay.dolphin.async.dnn.proto.NeuralNetworkProtos;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.util.Builder;

/**
 * Configuration builder for local response normalization(LRN) layer.
 *
 * The configuration that this builder generates is used to create a LRN layer instance.
 */
public final class LRNLayerConfigurationBuilder implements Builder<Configuration> {

  public static LRNLayerConfigurationBuilder newConfigurationBuilder() {
    return new LRNLayerConfigurationBuilder();
  }

  private int localSize = 5;
  private float alpha = 1;
  private float beta = 0.75f;
  private float k = 1;

  public synchronized LRNLayerConfigurationBuilder setLocalSize(final int localSize) {
    this.localSize = localSize;
    return this;
  }

  public synchronized LRNLayerConfigurationBuilder setAlpha(final float alpha) {
    this.alpha = alpha;
    return this;
  }

  public synchronized LRNLayerConfigurationBuilder setBeta(final float beta) {
    this.beta = beta;
    return this;
  }

  public synchronized LRNLayerConfigurationBuilder setK(final float k) {
    this.k = k;
    return this;
  }

  public synchronized LRNLayerConfigurationBuilder fromProtoConfiguration(
      final NeuralNetworkProtos.LayerConfiguration protoConf) {
    localSize = protoConf.getLrnParam().getLocalSize();
    alpha = protoConf.getLrnParam().getAlpha();
    beta = protoConf.getLrnParam().getBeta();
    k = protoConf.getLrnParam().getK();
    return this;
  }

  @Override
  public synchronized Configuration build() {
    if (localSize % 2 == 0) {
      throw new IllegalArgumentException("local size should be an odd number");
    } else {
      return Tang.Factory.getTang().newConfigurationBuilder()
          .bindNamedParameter(LayerConfigurationParameters.LocalSize.class, String.valueOf(localSize))
          .bindNamedParameter(LayerConfigurationParameters.Alpha.class, String.valueOf(alpha))
          .bindNamedParameter(LayerConfigurationParameters.Beta.class, String.valueOf(beta))
          .bindNamedParameter(LayerConfigurationParameters.K.class, String.valueOf(k))
          .bindImplementation(LayerBase.class, LRNGpuLayer.class)
          .build();
    }
  }
}
