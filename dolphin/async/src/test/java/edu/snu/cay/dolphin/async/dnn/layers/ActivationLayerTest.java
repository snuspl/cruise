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
import edu.snu.cay.dolphin.async.dnn.blas.MatrixFactory;
import edu.snu.cay.dolphin.async.dnn.blas.jblas.MatrixJBLASFactory;
import edu.snu.cay.dolphin.async.dnn.conf.ActivationLayerConfigurationBuilder;
import edu.snu.cay.dolphin.async.dnn.conf.LayerConfigurationParameters.*;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import static edu.snu.cay.dolphin.async.dnn.util.NeuralNetworkUtils.shapeToString;
import static org.junit.Assert.assertTrue;

/**
 * Test class for activation layer.
 */
public final class ActivationLayerTest {

  private static MatrixFactory matrixFactory;
  private static final float TOLERANCE = 1e-6f;

  static {
    final Configuration configuration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(MatrixFactory.class, MatrixJBLASFactory.class)
        .build();
    try {
      matrixFactory = Tang.Factory.getTang().newInjector(configuration).getInstance(MatrixFactory.class);
    } catch (final InjectionException e) {
      throw new RuntimeException("InjectionException while injecting a matrix factory: " + e);
    }
  }

  private final Matrix input = matrixFactory.create(new float[][]{
      {-1.0f, -0.5f, 0.5f, 1.0f},
      {-0.6f, -0.3f, 0.3f, 0.6f}});
  private final Matrix expectedSigmoidActivation = matrixFactory.create(new float[][]{
      {2.689414214e-01f, 3.775406688e-01f, 6.224593312e-01f, 7.310585786e-01f},
      {3.543436938e-01f, 4.255574832e-01f, 5.744425168e-01f, 6.456563062e-01f}});
  private final Matrix nextError = matrixFactory.create(new float[][]{
      {0.1f, 0.5f, -0.2f, 0.3f},
      {0.18f, -0.23f, 0.195f, -0.076f}});
  private final Matrix expectedSigmoidError = matrixFactory.create(new float[][]{
      {1.96611933241e-02f, 1.17501856101e-01f, -4.70007424403e-02f, 5.89835799724e-02f},
      {4.11811632822e-02f, -5.62254116889e-02f, 4.76693707797e-02f, -1.73876022747e-02f}});

  private LayerBase sigmoidActivationLayer;

  @Before
  public void setup() throws InjectionException {
    final Configuration layerConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(LayerIndex.class, String.valueOf(0))
        .bindNamedParameter(LayerInputShape.class, shapeToString(new int[]{input.getLength()}))
        .build();

    final Configuration sigmoidActivationLayerConf = ActivationLayerConfigurationBuilder.newConfigurationBuilder()
        .setActivationFunction("sigmoid")
        .build();

    this.sigmoidActivationLayer =
        Tang.Factory.getTang().newInjector(layerConf, sigmoidActivationLayerConf)
        .getInstance(LayerBase.class);
  }

  @Test
  public void testSigmoidActivation() {
    final Matrix activation = sigmoidActivationLayer.feedForward(input);
    assertTrue(expectedSigmoidActivation.compare(activation, TOLERANCE));
  }

  @Test
  public void testSigmoidBackPropagate() {
    final Matrix error = sigmoidActivationLayer.backPropagate(input, expectedSigmoidActivation, nextError);
    assertTrue(expectedSigmoidError.compare(error, TOLERANCE));
  }
}
