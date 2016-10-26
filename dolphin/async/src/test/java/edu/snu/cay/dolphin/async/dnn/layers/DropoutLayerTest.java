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

import edu.snu.cay.dolphin.async.dnn.TestDevice;
import edu.snu.cay.dolphin.async.dnn.blas.Matrix;
import edu.snu.cay.dolphin.async.dnn.blas.MatrixFactory;
import edu.snu.cay.dolphin.async.dnn.blas.MatrixUtils;
import edu.snu.cay.dolphin.async.dnn.blas.cuda.MatrixCudaFactory;
import edu.snu.cay.dolphin.async.dnn.blas.jblas.MatrixJBLASFactory;
import edu.snu.cay.dolphin.async.dnn.conf.DropoutLayerConfigurationBuilder;
import edu.snu.cay.dolphin.async.dnn.conf.LayerConfigurationParameters.LayerIndex;
import edu.snu.cay.dolphin.async.dnn.conf.LayerConfigurationParameters.LayerInputShape;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

/**
 * Test class for dropout layer.
 */
@RunWith(Parameterized.class)
public final class DropoutLayerTest {

  private static final float TOLERANCE = 1e-6f;

  @Parameterized.Parameters
  public static Object[] data() throws IOException {
    return TestDevice.getTestDevices();
  }

  private final boolean cpuOnly;

  private Matrix input;

  private Matrix nextError;

  private Matrix expectedDropoutActivation;

  private Matrix expectedDropoutError;

  private LayerBase dropoutLayer;

  public DropoutLayerTest(final String testDevice) throws InjectionException {
    this.cpuOnly = testDevice.equals(TestDevice.CPU);
  }

  @Before
  public void setup() throws InjectionException {
    final Configuration blasConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(MatrixFactory.class,
            cpuOnly ? MatrixJBLASFactory.class : MatrixCudaFactory.class)
        .build();
    final MatrixFactory matrixFactory = Tang.Factory.getTang().newInjector(blasConf).getInstance(MatrixFactory.class);

    this.input = matrixFactory.create(new float[][]{
        {0.4f, -2.0f},
        {-0.5f, -4.5f},
        {-0.2f, 1.6f},
        {-0.7f, 1.4f}});

    this.nextError = matrixFactory.create(new float[][]{
        {0.1f, 0},
        {0.3f, 0.3f},
        {0.6f, 0.4f},
        {0.4f, 0.1f}});

    this.expectedDropoutActivation = matrixFactory.create(new float[][] {
        {0, -4.0f},
        {-1.0f, 0},
        {0, 3.2f},
        {-1.4f, 2.8f}});

    this.expectedDropoutError = matrixFactory.create(new float[][] {
        {0, 0},
        {0.6f, 0},
        {0, 0.8f},
        {0.8f, 0.2f}});

    final Configuration layerConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(LayerIndex.class, String.valueOf(0))
        .bindNamedParameter(LayerInputShape.class, "4,1,1")
        .build();

    final DropoutLayerConfigurationBuilder builder = DropoutLayerConfigurationBuilder
        .newConfigurationBuilder()
        .setDropoutRatio(0.5f);

    final Injector injector = Tang.Factory.getTang().newInjector(blasConf);
    final MatrixFactory matrixFactoryForLayer = injector.getInstance(MatrixFactory.class);

    matrixFactoryForLayer.setRandomSeed(10);
    this.dropoutLayer = injector.forkInjector(layerConf, builder.build())
        .getInstance(LayerBase.class);
  }

  @After
  public void tearDown() {
    dropoutLayer.cleanup();
    MatrixUtils.free(input);
    MatrixUtils.free(nextError);
    MatrixUtils.free(expectedDropoutActivation);
    MatrixUtils.free(expectedDropoutError);
  }

  @Test
  public void testDropout() {
    final Matrix output = dropoutLayer.feedForward(input);
    assertTrue(output != null);
    assertTrue(expectedDropoutActivation.compare(output, TOLERANCE));
    final Matrix error = dropoutLayer.backPropagate(input, expectedDropoutActivation, nextError);
    assertTrue(expectedDropoutError.compare(error, TOLERANCE));
  }
}
