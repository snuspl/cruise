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
package edu.snu.cay.dolphin.async.dnn.data;

import edu.snu.cay.dolphin.async.dnn.blas.Matrix;
import edu.snu.cay.dolphin.async.dnn.blas.MatrixFactory;
import edu.snu.cay.dolphin.async.dnn.blas.jblas.MatrixJBLASFactory;
import edu.snu.cay.dolphin.async.dnn.layers.LayerParameter;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * Test class for testing {@link LayerParameterCodec}'s encoding and decoding features.
 */
public final class LayerParameterCodecTest {

  private LayerParameterCodec layerParameterCodec;
  private MatrixFactory matrixFactory;

  @Before
  public void setUp() throws InjectionException {
    final Configuration conf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(MatrixFactory.class, MatrixJBLASFactory.class)
        .build();
    final Injector injector = Tang.Factory.getTang().newInjector(conf);

    this.layerParameterCodec = injector.getInstance(LayerParameterCodec.class);
    this.matrixFactory = injector.getInstance(MatrixFactory.class);
  }

  /**
   * Checks that a random layer parameter array does not change after encoding and decoding it, sequentially.
   */
  @Test
  public void testEncodeDecodeLayerParameters() {
    final LayerParameter inputLayerParameter = generateRandomLayerParameter(matrixFactory);
    final LayerParameter outputLayerParameter =
        layerParameterCodec.decode(layerParameterCodec.encode(inputLayerParameter));

    assertEquals("Encode-decode result is different from the expected LayerParameter",
        inputLayerParameter, outputLayerParameter);
  }

  public static LayerParameter generateRandomLayerParameter(final MatrixFactory matrixFactory) {
    return generateRandomLayerParameter(matrixFactory, new Random());
  }

  /**
   * @param random a random number generator.
   * @return a random layer parameter.
   */
  public static LayerParameter generateRandomLayerParameter(final MatrixFactory matrixFactory, final Random random) {
    final Matrix weightParam = MatrixGenerator.generateRandomMatrix(matrixFactory, random);
    final Matrix biasParam = MatrixGenerator.generateRandomMatrix(matrixFactory, random);
    return LayerParameter.newBuilder()
        .setWeightParam(weightParam)
        .setBiasParam(biasParam).build();
  }
}
