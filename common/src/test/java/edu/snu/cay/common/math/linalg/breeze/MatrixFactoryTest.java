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
package edu.snu.cay.common.math.linalg.breeze;

import edu.snu.cay.common.math.linalg.Matrix;
import edu.snu.cay.common.math.linalg.MatrixFactory;
import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.common.math.linalg.VectorFactory;
import org.apache.commons.lang.ArrayUtils;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * This tests {@link DefaultMatrixFactory}.
 */
public final class MatrixFactoryTest {

  private MatrixFactory matrixFactory;
  private VectorFactory vectorFactory;

  @Before
  public void setUp() {
    try {
      matrixFactory = Tang.Factory.getTang().newInjector().getInstance(DefaultMatrixFactory.class);
      vectorFactory = Tang.Factory.getTang().newInjector().getInstance(VectorFactory.class);
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Tests {@link DefaultMatrixFactory} creates {@link DenseMatrix} as intended.
   */
  @Test
  public void testDenseMatrix() {
    final double[] value = {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2};
    final Vector vec1 = vectorFactory.createDense(value);
    final Vector vec2 = vectorFactory.createDense(value);
    final List<Vector> denseVectorList = new ArrayList<>();
    denseVectorList.add(vec1);
    denseVectorList.add(vec2);

    final Matrix mat1 = matrixFactory.createDenseZeros(3, 4);
    final Matrix mat2 = matrixFactory.createDense(3, 4, value);
    final Matrix mat3 = matrixFactory.createDense(12, 2, ArrayUtils.addAll(value, value));

    assertEquals(mat1.size(), 12);
    assertEquals(mat2.size(), 12);
    assertEquals(mat3.size(), 24);

    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 4; j++) {
        assertEquals(mat1.get(i, j), 0.0, 0.0);
        assertEquals(mat2.get(i, j), value[i + j * 3], 0.0);
      }
    }

    assertEquals(matrixFactory.horzcatVecDense(denseVectorList), mat3);
    assertArrayEquals(((DenseMatrix) mat3).toArray(), ArrayUtils.addAll(value, value), 0.0);

    final List<Matrix> denseMatrixList = new ArrayList<>();
    denseMatrixList.add(mat1);
    denseMatrixList.add(mat2);
    final Matrix horzMat = matrixFactory.horzcatMatDense(denseMatrixList);
    final Matrix vertMat = matrixFactory.vertcatMatDense(denseMatrixList);

    for (int i = 0; i < horzMat.getColumns(); i++) {
      if (i < mat1.getColumns()) {
        assertEquals(mat1.sliceColumn(i), horzMat.sliceColumn(i));
      } else {
        assertEquals(mat2.sliceColumn(i - mat1.getColumns()), horzMat.sliceColumn(i));
      }
    }

    for (int i = 0; i < vertMat.getRows(); i++) {
      if (i < mat1.getRows()) {
        assertEquals(mat1.sliceRow(i), vertMat.sliceRow(i));
      } else {
        assertEquals(mat2.sliceRow(i - mat1.getRows()), vertMat.sliceRow(i));
      }
    }

    assertEquals(mat1, horzMat.sliceColumns(0, 4));
    assertEquals(mat2, horzMat.sliceColumns(4, 8));
    assertEquals(mat1, vertMat.sliceRows(0, 3));
    assertEquals(mat2, vertMat.sliceRows(3, 6));
  }

  /**
   * Tests {@link DefaultMatrixFactory} creates {@link CSCMatrix} as intended.
   */
  @Test
  public void testCSCMatrix() {
    final int[][] index = {{0, 2, 4, 6}, {1, 2, 3, 4, 5}, {3, 5, 7}};
    final double[][] value = {{0.1, 0.2, 0.3, 0.4}, {0.5, 0.6, 0.7, 0.8, 0.9}, {1.0, 1.1, 1.2}};
    final Vector vec1 = vectorFactory.createSparse(index[0], value[0], 10);
    final Vector vec2 = vectorFactory.createSparse(index[1], value[1], 10);
    final Vector vec3 = vectorFactory.createSparse(index[2], value[2], 10);
    final List<Vector> sparseVectorList = new ArrayList<>();
    sparseVectorList.add(vec1);
    sparseVectorList.add(vec2);
    sparseVectorList.add(vec3);

    final Matrix mat1 = matrixFactory.createCSCZeros(10, 3, 0);
    final Matrix mat2 = matrixFactory.horzcatVecSparse(sparseVectorList);

    assertEquals(mat1.size(), 30);
    assertEquals(mat2.size(), 30);

    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < index[i].length; j++) {
        assertEquals(mat2.get(index[i][j], i), value[i][j], 0.0);
      }
    }
  }
}
