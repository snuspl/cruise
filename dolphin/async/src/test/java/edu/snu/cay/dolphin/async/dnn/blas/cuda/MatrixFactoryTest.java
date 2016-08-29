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
package edu.snu.cay.dolphin.async.dnn.blas.cuda;

import edu.snu.cay.dolphin.async.dnn.blas.Matrix;
import edu.snu.cay.dolphin.async.dnn.blas.MatrixFactory;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import static edu.snu.cay.dolphin.async.dnn.blas.cuda.FloatCompare.assertArrayEquals;
import static edu.snu.cay.dolphin.async.dnn.blas.cuda.FloatCompare.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link MatrixCudaFactory}.
 */
public final class MatrixFactoryTest {
  private MatrixFactory matrixFactory;

  @Before
  public void setUp() {
    try {
      matrixFactory = Tang.Factory.getTang().newInjector().getInstance(MatrixCudaFactory.class);
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Tests matrix creation with given data.
   */
  @Test
  public void testMatrix() {
    final float[] data1 = {0.1F, 0.2F, 0.3F, 0.4F, 0.5F, 0.6F};
    final float[][] data2 = {{0.1F, 0.3F, 0.5F}, {0.2F, 0.4F, 0.6F}};

    final Matrix m1 = matrixFactory.create(data1);
    assertEquals(6, m1.getRows());
    assertEquals(1, m1.getColumns());
    assertEquals(6, m1.getLength());
    final float[] m1Array = m1.toFloatArray();
    assertArrayEquals(data1, m1Array);
    m1.free();

    final Matrix m2 = matrixFactory.create(data1, 2, 3);
    assertEquals(2, m2.getRows());
    assertEquals(3, m2.getColumns());
    assertEquals(6, m1.getLength());
    final float[] m2Array = m2.toFloatArray();
    assertArrayEquals(data1, m2Array);
    m2.free();

    final Matrix m3 = matrixFactory.create(data2);
    assertEquals(2, m3.getRows());
    assertEquals(3, m3.getColumns());
    assertEquals(6, m1.getLength());
    final float[] m3Array = m3.toFloatArray();
    assertArrayEquals(data1, m3Array);
    m3.free();
  }

  /**
   * Tests matrix creation with zero filling.
   */
  @Test
  public void testMatrixZeros() {
    final Matrix m1 = matrixFactory.zeros(4);
    assertEquals(4, m1.getRows());
    assertEquals(1, m1.getColumns());
    assertTrue(m1.isColumnVector());
    final float[] m1Array = m1.toFloatArray();
    for (int i = 0; i < 4; i++) {
      assertEquals(0.0F, m1Array[i]);
    }
    m1.free();

    final Matrix m2 = matrixFactory.zeros(3, 4);
    assertEquals(3, m2.getRows());
    assertEquals(4, m2.getColumns());
    assertTrue(!m2.isColumnVector());
    final float[] m2Array = m2.toFloatArray();
    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 3; j++) {
        assertEquals(0.0F, m2Array[i * 3 + j]);
      }
    }
    m2.free();
  }

  /**
   * Tests matrix creation with one filling.
   */
  @Test
  public void testMatrixOnes() {
    final Matrix m1 = matrixFactory.ones(4);
    assertEquals(4, m1.getRows());
    assertEquals(1, m1.getColumns());
    assertTrue(m1.isColumnVector());
    final float[] m1Array = m1.toFloatArray();
    for (int i = 0; i < 4; i++) {
      assertEquals(1.0F, m1Array[i]);
    }
    m1.free();

    final Matrix m2 = matrixFactory.ones(3, 4);
    assertEquals(3, m2.getRows());
    assertEquals(4, m2.getColumns());
    assertTrue(!m2.isColumnVector());
    final float[] m2Array = m2.toFloatArray();
    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 3; j++) {
        assertEquals(1.0F, m2Array[i * 3 + j]);
      }
    }
    m2.free();
  }

  /**
   * Tests matrix concatenation.
   */
  @Test
  public void testMatrixConcat() {
    final float[] input1 = {0.1F, 0.2F, 0.3F, 0.4F, 0.5F, 0.6F};
    final float[] input2 = {0.7F, 0.8F, 0.9F, 1.0F, 1.1F, 1.2F};
    final float[] output1 = {0.1F, 0.2F, 0.3F, 0.4F, 0.5F, 0.6F, 0.7F, 0.8F, 0.9F, 1.0F, 1.1F, 1.2F};
    final float[] output2 = {0.1F, 0.2F, 0.3F, 0.7F, 0.8F, 0.9F, 0.4F, 0.5F, 0.6F, 1.0F, 1.1F, 1.2F};
    final Matrix m1 = matrixFactory.create(input1, 3, 2);
    final Matrix m2 = matrixFactory.create(input2, 3, 2);

    final Matrix m3 = matrixFactory.concatHorizontally(m1, m2);
    assertEquals(3, m3.getRows());
    assertEquals(4, m3.getColumns());
    final float[] m3Array = m3.toFloatArray();
    assertArrayEquals(output1, m3Array);
    m3.free();

    final Matrix m4 = matrixFactory.concatVertically(m1, m2);
    assertEquals(6, m4.getRows());
    assertEquals(2, m4.getColumns());
    final float[] m4Array = m4.toFloatArray();
    assertArrayEquals(output2, m4Array);
    m4.free();

    m1.free();
    m2.free();
  }
}
