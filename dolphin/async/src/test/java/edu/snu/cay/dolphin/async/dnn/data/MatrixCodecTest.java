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

import edu.snu.cay.dolphin.async.dnn.TestDevice;
import edu.snu.cay.dolphin.async.dnn.blas.Matrix;
import edu.snu.cay.dolphin.async.dnn.blas.MatrixFactory;
import edu.snu.cay.dolphin.async.dnn.blas.MatrixUtils;
import edu.snu.cay.dolphin.async.dnn.blas.cuda.MatrixCudaFactory;
import edu.snu.cay.dolphin.async.dnn.blas.jblas.MatrixJBLASFactory;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * Test class for testing {@link MatrixCodec}'s encoding and decoding functions.
 */
@RunWith(Parameterized.class)
public final class MatrixCodecTest {

  @Parameterized.Parameters
  public static Object[] data() throws IOException {
    return TestDevice.getTestDevices();
  }

  private String testDevice;
  private MatrixCodec matrixCodec;
  private Random random;
  private MatrixFactory matrixFactory;

  public MatrixCodecTest(final String testDevice) {
    this.testDevice = testDevice;
  }

  @Before
  public void setUp() throws InjectionException {
    final Configuration conf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(MatrixFactory.class,
            testDevice.equals(TestDevice.CPU) ? MatrixJBLASFactory.class : MatrixCudaFactory.class)
        .build();
    final Injector injector = Tang.Factory.getTang().newInjector(conf);

    this.matrixCodec = injector.getInstance(MatrixCodec.class);
    this.random = new Random();
    this.matrixFactory = injector.getInstance(MatrixFactory.class);
  }

  /**
   * Checks that a random row vector does not change after encoding and decoding it, sequentially.
   */
  @Test
  public void testEncodeDecodeVector() {
    final Matrix inputVector = MatrixGenerator.generateRandomVector(matrixFactory, random);
    final Matrix retVector = matrixCodec.decode(matrixCodec.encode(inputVector));

    try {
      assertEquals(inputVector, retVector);
    } finally {
      MatrixUtils.free(inputVector);
      MatrixUtils.free(retVector);
    }
  }

  /**
   * Checks that a random matrix does not change after encoding and decoding it, sequentially.
   */
  @Test
  public void testEncodeDecodeMatrix() {
    final Matrix inputMatrix = MatrixGenerator.generateRandomMatrix(matrixFactory, random);
    final Matrix retMatrix = matrixCodec.decode(matrixCodec.encode(inputMatrix));

    try {
      assertEquals("Encode-decode result is different from expected array", inputMatrix, retMatrix);
    } finally {
      MatrixUtils.free(inputMatrix);
      MatrixUtils.free(retMatrix);
    }
  }
}
