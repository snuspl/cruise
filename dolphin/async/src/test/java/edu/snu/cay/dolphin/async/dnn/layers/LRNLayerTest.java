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
import edu.snu.cay.dolphin.async.dnn.conf.*;
import edu.snu.cay.dolphin.async.dnn.conf.LayerConfigurationParameters.*;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Test class for lrn layer.
 */
public class LRNLayerTest {

  private static MatrixFactory matrixFactory;
  private static final float TOLERANCE = 1e-4f;

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
  public void setup() throws InjectionException {
    final Configuration layerConfSizeTwo = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(LayerIndex.class, String.valueOf(0))
        .bindNamedParameter(LayerInputShape.class, "2,2,1")
        .bindImplementation(MatrixFactory.class, MatrixJBLASFactory.class)
        .build();

    final Configuration layerConfSizeOne = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(LayerIndex.class, String.valueOf(0))
        .bindNamedParameter(LayerInputShape.class, "4,1,1")
        .bindImplementation(MatrixFactory.class, MatrixJBLASFactory.class)
        .build();

    final LRNLayerConfigurationBuilder builder = LRNLayerConfigurationBuilder
        .newConfigurationBuilder()
        .setAlpha(3)
        .setBeta(2)
        .setK(1)
        .setLocalSize(3);

    this.lrnLayerSizeTwo =
        Tang.Factory.getTang().newInjector(layerConfSizeTwo, builder.build())
            .getInstance(LayerBase.class);

    this.lrnLayerSizeOne =
        Tang.Factory.getTang().newInjector(layerConfSizeOne, builder.build())
            .getInstance(LayerBase.class);
  }

  @Test
  public void testLRNSizeTwo() {
    final Matrix activation = lrnLayerSizeTwo.feedForward(input);
    assertTrue(expectedLRNActivationSizeTwo.compare(activation, TOLERANCE));
    final Matrix error = lrnLayerSizeTwo.backPropagate(input, expectedLRNActivationSizeTwo, nextError);
    assertTrue(expectedLRNErrorSizeTwo.compare(error, TOLERANCE));
  }

  @Test
  public void testLRNSizeOne() {
    final Matrix activation = lrnLayerSizeOne.feedForward(input);
    assertTrue(expectedLRNActivationSizeOne.compare(activation, TOLERANCE));
    final Matrix error = lrnLayerSizeOne.backPropagate(input, expectedLRNActivationSizeOne, nextError);
    assertTrue(expectedLRNErrorSizeOne.compare(error, TOLERANCE));
  }
}
