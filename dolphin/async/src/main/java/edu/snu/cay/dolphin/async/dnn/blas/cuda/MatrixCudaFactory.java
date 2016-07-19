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

import static edu.snu.cay.dolphin.async.dnn.blas.cuda.MatrixCudaImpl.FLOAT_SIZE;

/**
 * Factory for CUDA backend Matrix implementation.
 */
public final class MatrixCudaFactory implements MatrixFactory {

  @Override
  public Matrix create(final int length) {
    return new MatrixCudaImpl(length, 1);
  }

  @Override
  public Matrix create(final int rows, final int columns) {
    return new MatrixCudaImpl(rows, columns);
  }

  @Override
  public Matrix create(final float[] data) {
    return new MatrixCudaImpl(data);
  }

  @Override
  public Matrix create(final float[][] data) {
    return new MatrixCudaImpl(data);
  }

  @Override
  public Matrix create(final float[] data, final int rows, final int columns) {
    if (data.length != rows * columns) {
      throw new IllegalArgumentException(
          "The length of input data array is not equal to the product of the number of rows and columns.");
    }
    return new MatrixCudaImpl(data, rows, columns);
  }

  @Override
  public Matrix ones(final int length) {
    final Matrix newMatrix = create(length);
    return newMatrix.fill(1.0F);
  }

  @Override
  public Matrix ones(final int rows, final int columns) {
    final Matrix newMatrix = create(rows, columns);
    return newMatrix.fill(1.0F);
  }

  @Override
  public Matrix zeros(final int length) {
    final Matrix newMatrix = create(length);
    return newMatrix.fill(0.0F);
  }

  @Override
  public Matrix zeros(final int rows, final int columns) {
    final Matrix newMatrix = create(rows, columns);
    return newMatrix.fill(0.0F);
  }

  @Override
  public void setRandomSeed(final long seed) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Matrix rand(final int length) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Matrix rand(final int rows, final int columns) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Matrix rand(final int rows, final int columns, final long seed) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Matrix randn(final int length) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Matrix randn(final int rows, final int columns) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Matrix randn(final int rows, final int columns, final long seed) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Matrix bernoulli(final int rows, final int columns, final float prob) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Matrix bernoulli(final int rows, final int columns, final float prob, final float scale) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Matrix concatHorizontally(final Matrix a, final Matrix b) {
    final MatrixCudaImpl m1 = MatrixCudaImpl.toCudaImpl(a);
    final MatrixCudaImpl m2 = MatrixCudaImpl.toCudaImpl(b);
    if (m1.getRows() != m2.getRows()) {
      throw new IllegalArgumentException(
          "Cannot concat horizontally. The number of rows is different: " + m1.getRows() + ", " + m2.getRows());
    }
    final MatrixCudaImpl newMatrix = new MatrixCudaImpl(m1.getRows(), m1.getColumns() + m2.getColumns());
    if (!JavaCuda.d2dMemcpy(newMatrix.getDevicePointer(), m1.getDevicePointer(), FLOAT_SIZE * m1.getLength())) {
      newMatrix.free();
      throw new RuntimeException("Failed to copy GPU memory");
    } else if (!JavaCuda.d2dMemcpy(newMatrix.getDevicePointer().position(FLOAT_SIZE * m1.getLength()),
        m2.getDevicePointer(), FLOAT_SIZE * m2.getLength())) {
      newMatrix.free();
      throw new RuntimeException("Failed to copy GPU memory");
    }
    newMatrix.getDevicePointer().position(0);
    return newMatrix;
  }

  @Override
  public Matrix concatVertically(final Matrix a, final Matrix b) {
    final MatrixCudaImpl m1 = MatrixCudaImpl.toCudaImpl(a);
    final MatrixCudaImpl m2 = MatrixCudaImpl.toCudaImpl(b);
    if (m1.getColumns() != m2.getColumns()) {
      throw new IllegalArgumentException(
          "Cannot concat vertically. The number of columns is different: " + m1.getColumns() + ", " + m2.getColumns());
    }
    final MatrixCudaImpl newMatrix = new MatrixCudaImpl(m1.getRows() + m2.getRows(), m1.getColumns());
    if (!JavaCuda.d2dMemcpy2D(newMatrix.getDevicePointer(), FLOAT_SIZE * newMatrix.getRows(),
        m1.getDevicePointer(), FLOAT_SIZE * m1.getRows(), FLOAT_SIZE * m1.getRows(), m1.getColumns())) {
      newMatrix.free();
      throw new RuntimeException("Failed to copy GPU memory");
    } else if (!JavaCuda.d2dMemcpy2D(newMatrix.getDevicePointer().position(FLOAT_SIZE * m1.getRows()),
        FLOAT_SIZE * newMatrix.getRows(), m2.getDevicePointer(),
        FLOAT_SIZE * m2.getRows(), FLOAT_SIZE * m2.getRows(), m2.getColumns())) {
      newMatrix.free();
      throw new RuntimeException("Failed to copy GPU memory");
    }
    newMatrix.getDevicePointer().position(0);
    return newMatrix;
  }
}
