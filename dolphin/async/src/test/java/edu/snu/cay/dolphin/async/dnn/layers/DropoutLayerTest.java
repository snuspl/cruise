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
import edu.snu.cay.dolphin.async.dnn.conf.DropoutLayerConfigurationBuilder;
import edu.snu.cay.dolphin.async.dnn.conf.LayerConfigurationParameters.*;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Test class for dropout layer.
 */
public final class DropoutLayerTest {

  private static MatrixFactory matrixFactory;
  private static final float TOLERANCE = 1e-6f;
  private static final Configuration MATRIX_CONF = Tang.Factory.getTang().newConfigurationBuilder()
      .bindImplementation(MatrixFactory.class, MatrixJBLASFactory.class)
      .build();

  static {
    try {
      matrixFactory = Tang.Factory.getTang().newInjector(MATRIX_CONF).getInstance(MatrixFactory.class);
    } catch (final InjectionException e) {
      throw new RuntimeException("InjectionException while injecting a matrix factory: " + e);
    }
  }

  private final Matrix input = matrixFactory.create(new float[][]{
      {0.4f, -2.0f},
      {-0.5f, -4.5f},
      {-0.2f, 1.6f},
      {-0.7f, 1.4f}});

  private final Matrix nextError = matrixFactory.create(new float[][]{
      {0.1f, 0},
      {0.3f, 0.3f},
      {0.6f, 0.4f},
      {0.4f, 0.1f}});

  private LayerBase dropoutLayer;

  @Before
  public void setup() throws InjectionException {
    final Configuration layerConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(LayerIndex.class, String.valueOf(0))
        .bindNamedParameter(LayerInputShape.class, "4,2")
        .build();

    final DropoutLayerConfigurationBuilder builder = DropoutLayerConfigurationBuilder
        .newConfigurationBuilder()
        .setDropoutRatio(0.5f);

    final Injector injector = Tang.Factory.getTang().newInjector(MATRIX_CONF);

    this.dropoutLayer = injector.forkInjector(layerConf, builder.build())
        .getInstance(LayerBase.class);
  }

  @After
  public void tearDown() {
    dropoutLayer.cleanup();
  }

  @Test
  public void testDropout() {
    final float scale = 2.0f;
    final Matrix output = dropoutLayer.feedForward(input);
    final Matrix expectedDropoutError = matrixFactory.zeros(input.getRows(), input.getColumns());
    for (int r = 0; r < output.getRows(); r++) {
      for (int c = 0; c < output.getColumns(); c++) {
        final float expectedDropoutActivation = input.get(r, c) * scale;
        final float dropoutActivation = output.get(r, c);
        assertTrue(dropoutActivation == 0 || dropoutActivation == expectedDropoutActivation);
        if (dropoutActivation != 0) {
          expectedDropoutError.put(r, c, scale);
        }
      }
    }
    expectedDropoutError.muli(nextError);
    final Matrix error = dropoutLayer.backPropagate(input, output, nextError);
    assertTrue(expectedDropoutError.compare(error, TOLERANCE));
  }
}
