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
package edu.snu.cay.dolphin.async.dnn.layers.cuda;

import edu.snu.cay.dolphin.async.dnn.blas.Matrix;
import edu.snu.cay.dolphin.async.dnn.blas.MatrixFactory;
import edu.snu.cay.dolphin.async.dnn.blas.cuda.MatrixCudaFactory;
import edu.snu.cay.dolphin.async.dnn.conf.*;
import edu.snu.cay.dolphin.async.dnn.conf.LayerConfigurationParameters.*;
import edu.snu.cay.dolphin.async.dnn.conf.NeuralNetworkConfigurationParameters;
import edu.snu.cay.dolphin.async.dnn.layers.LayerBase;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Class for testing Gpu activation with loss layer.
 */
public final class ActivationWithLossGpuLayerTest {

  private static MatrixFactory matrixFactory;
  private static final float TOLERANCE = 1e-6f;
  private static final Configuration MATRIX_CONF = Tang.Factory.getTang().newConfigurationBuilder()
      .bindImplementation(MatrixFactory.class, MatrixCudaFactory.class)
      .build();

  static {
    try {
      matrixFactory = Tang.Factory.getTang().newInjector(MATRIX_CONF).getInstance(MatrixFactory.class);
    } catch (final InjectionException e) {
      throw new RuntimeException("InjectionException while injecting a matrix factory: " + e);
    }
  }

  private Matrix input;
  private Matrix expectedOutput;
  private LayerBase activationWithLossLayer;

  @Before
  public void setup() {
    try {
      final Configuration conf = Tang.Factory.getTang().newConfigurationBuilder()
          .bindNamedParameter(LayerIndex.class, String.valueOf(0))
          .bindNamedParameter(LayerInputShape.class, "3,1,1")
          .bindNamedParameter(NeuralNetworkConfigurationParameters.BatchSize.class, "2")
          .build();
      final Configuration activationWithLossLayerConf =
          ActivationWithLossLayerConfigurationBuilder.newConfigurationBuilder()
              .setActivationFunction("softmax")
              .setLossFunction("crossEntropy")
              .build();

      final int numInput = 3;
      final int numBatch = 2;
      this.input = matrixFactory.create(new float[]{
          1.0f, -2.0f, 3.0f,
          -4.0f, 5.0f, -6.0f},
          numInput, numBatch);
      this.expectedOutput = matrixFactory.create(new float[]{
          1.184996545e-01f, 5.899750402e-03f, 8.756005951e-01f,
          1.233925154e-04f, 9.998599081e-01f, 1.669936102e-05f},
          numInput, numBatch);

      final Injector injector = Tang.Factory.getTang().newInjector(MATRIX_CONF);

      this.activationWithLossLayer =
          injector.forkInjector(conf, activationWithLossLayerConf).getInstance(LayerBase.class);
    } catch (final InjectionException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testSoftmaxApply() {
    assertTrue(expectedOutput.compare(activationWithLossLayer.feedForward(input), TOLERANCE));
  }
}

