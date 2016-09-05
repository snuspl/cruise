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
import edu.snu.cay.dolphin.async.dnn.layers.LayerBase;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Test class for GPU lrn layer.
 */
public class LRNGpuLayerTest {

  private static MatrixFactory matrixFactory;
  private static final float TOLERANCE = 1e-4f;
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

  private final Matrix input = matrixFactory.create(new float[][]{
      {1, 1},
      {1, 1},
      {1, 1},
      {1, 1}});

  private final Matrix nextError = matrixFactory.create(new float[][]{
      {1, 1},
      {1, 1},
      {1, 1},
      {1, 1}});

  private final Matrix expectedLRNActivationSizeTwo = matrixFactory.create(new float[][] {
      {0.111111f, 0.111111f},
      {0.111111f, 0.111111f},
      {0.111111f, 0.111111f},
      {0.111111f, 0.111111f}});

  private final Matrix expectedLRNActivationSizeOne = matrixFactory.create(new float[][] {
      {0.111111f, 0.111111f},
      {0.0625f, 0.0625f},
      {0.0625f, 0.0625f},
      {0.111111f, 0.111111f}});

  private final Matrix expectedLRNErrorSizeTwo = matrixFactory.create(new float[][] {
      {-0.185185f, -0.185185f},
      {-0.185185f, -0.185185f},
      {-0.185185f, -0.185185f},
      {-0.185185f, -0.185185f}});

  private final Matrix expectedLRNErrorSizeOne = matrixFactory.create(new float[][] {
      {-0.099537f, -0.099537f},
      {-0.210648f, -0.210648f},
      {-0.210648f, -0.210648f},
      {-0.099537f, -0.099537f}});

  private LayerBase lrnLayerSizeTwo;
  private LayerBase lrnLayerSizeOne;

  @Before
  public void setup() {
    try {
      final Configuration layerConfSizeTwo = Tang.Factory.getTang().newConfigurationBuilder()
          .bindNamedParameter(LayerIndex.class, String.valueOf(0))
          .bindNamedParameter(LayerInputShape.class, "2,2,1")
          .bindNamedParameter(NeuralNetworkConfigurationParameters.BatchSize.class, "2")
          .build();

      final Configuration layerConfSizeOne = Tang.Factory.getTang().newConfigurationBuilder()
          .bindNamedParameter(LayerIndex.class, String.valueOf(0))
          .bindNamedParameter(LayerInputShape.class, "4,1,1")
          .bindNamedParameter(NeuralNetworkConfigurationParameters.BatchSize.class, "2")
          .build();

      final LRNLayerConfigurationBuilder builder = LRNLayerConfigurationBuilder
          .newConfigurationBuilder()
          .setAlpha(3)
          .setBeta(2)
          .setK(1)
          .setLocalSize(3)
          .setUseGpu(true);

      final Injector injector = Tang.Factory.getTang().newInjector(MATRIX_CONF);

      this.lrnLayerSizeTwo =
          injector.forkInjector(layerConfSizeTwo, builder.build())
              .getInstance(LayerBase.class);

      this.lrnLayerSizeOne =
          injector.forkInjector(layerConfSizeOne, builder.build())
              .getInstance(LayerBase.class);
    } catch (final InjectionException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testLRNSizeTwoFeedForward() {
    final Matrix activation = lrnLayerSizeTwo.feedForward(input);
    assertTrue(expectedLRNActivationSizeTwo.compare(activation, TOLERANCE));
  }

  @Test
  public void testLRNSizeTwoBackPropagate() {
    final Matrix error = lrnLayerSizeTwo.backPropagate(input, expectedLRNActivationSizeTwo, nextError);
    assertTrue(expectedLRNErrorSizeTwo.compare(error, TOLERANCE));
  }

  @Test
  public void testLRNSizeOneFeedForward() {
    final Matrix activation = lrnLayerSizeOne.feedForward(input);
    assertTrue(expectedLRNActivationSizeOne.compare(activation, TOLERANCE));
  }

  @Test
  public void testLRNSizeOneBackPropagate() {
    final Matrix error = lrnLayerSizeOne.backPropagate(input, expectedLRNActivationSizeOne, nextError);
    assertTrue(expectedLRNErrorSizeOne.compare(error, TOLERANCE));
  }
}
