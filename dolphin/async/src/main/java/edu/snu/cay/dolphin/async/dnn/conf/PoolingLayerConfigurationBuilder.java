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

import edu.snu.cay.dolphin.async.dnn.layerparam.initializer.LayerParameterInitializer;
import edu.snu.cay.dolphin.async.dnn.layerparam.initializer.PoolingLayerParameterInitializer;
import edu.snu.cay.dolphin.async.dnn.layers.LayerBase;
import edu.snu.cay.dolphin.async.dnn.layers.PoolingLayer;
import edu.snu.cay.dolphin.async.dnn.layers.cuda.PoolingGpuLayer;
import edu.snu.cay.dolphin.async.dnn.proto.NeuralNetworkProtos;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.util.Builder;

/**
 * Configuration builder for Pooling layer.
 *
 * The configuration that this builder generates is used to create a pooling layer instance.
 * The generated configuration needs to bind the parameter for a layer input shape, to inject a layer instance.
 */
public final class PoolingLayerConfigurationBuilder implements Builder<Configuration> {

  public static PoolingLayerConfigurationBuilder newConfigurationBuilder() {
    return new PoolingLayerConfigurationBuilder();
  }

  private String poolingType = "MAX";
  private int paddingHeight = 0;
  private int paddingWidth = 0;
  private int strideHeight = 1;
  private int strideWidth = 1;
  private int kernelHeight;
  private int kernelWidth;
  private Class<? extends LayerBase> layerClass = PoolingGpuLayer.class;

  public synchronized PoolingLayerConfigurationBuilder setPoolingType(final String poolingType) {
    this.poolingType = poolingType;
    return this;
  }

  public synchronized PoolingLayerConfigurationBuilder setPaddingHeight(final int paddingHeight) {
    this.paddingHeight = paddingHeight;
    return this;
  }

  public synchronized PoolingLayerConfigurationBuilder setPaddingWidth(final int paddingWidth) {
    this.paddingWidth = paddingWidth;
    return this;
  }

  public synchronized PoolingLayerConfigurationBuilder setStrideHeight(final int strideHeight) {
    this.strideHeight = strideHeight;
    return this;
  }

  public synchronized PoolingLayerConfigurationBuilder setStrideWidth(final int strideWidth) {
    this.strideWidth = strideWidth;
    return this;
  }

  public synchronized PoolingLayerConfigurationBuilder setKernelHeight(final int kernelHeight) {
    this.kernelHeight = kernelHeight;
    return this;
  }

  public synchronized PoolingLayerConfigurationBuilder setKernelWidth(final int kernelWidth) {
    this.kernelWidth = kernelWidth;
    return this;
  }

  public synchronized PoolingLayerConfigurationBuilder setCpuOnly(final boolean cpuOnly) {
    if (cpuOnly) {
      layerClass = PoolingLayer.class;
    } else {
      layerClass = PoolingGpuLayer.class;
    }
    return this;
  }

  public synchronized PoolingLayerConfigurationBuilder fromProtoConfiguration(
      final NeuralNetworkProtos.LayerConfiguration protoConf) {
    poolingType = protoConf.getPoolingParam().getPoolingType();
    paddingHeight = protoConf.getPoolingParam().getPaddingHeight();
    paddingWidth = protoConf.getPoolingParam().getPaddingWidth();
    strideHeight = protoConf.getPoolingParam().getStrideHeight();
    strideWidth = protoConf.getPoolingParam().getStrideWidth();
    kernelHeight = protoConf.getPoolingParam().getKernelHeight();
    kernelWidth = protoConf.getPoolingParam().getKernelWidth();
    return this;
  }

  @Override
  public synchronized Configuration build() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(LayerConfigurationParameters.PoolingType.class, poolingType)
        .bindNamedParameter(LayerConfigurationParameters.PaddingHeight.class, String.valueOf(paddingHeight))
        .bindNamedParameter(LayerConfigurationParameters.PaddingWidth.class, String.valueOf(paddingWidth))
        .bindNamedParameter(LayerConfigurationParameters.StrideHeight.class, String.valueOf(strideHeight))
        .bindNamedParameter(LayerConfigurationParameters.StrideWidth.class, String.valueOf(strideWidth))
        .bindNamedParameter(LayerConfigurationParameters.KernelHeight.class, String.valueOf(kernelHeight))
        .bindNamedParameter(LayerConfigurationParameters.KernelWidth.class, String.valueOf(kernelWidth))
        .bindImplementation(LayerBase.class, layerClass)
        .bindImplementation(LayerParameterInitializer.class, PoolingLayerParameterInitializer.class)
        .build();
  }
}
