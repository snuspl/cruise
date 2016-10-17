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
package edu.snu.cay.dolphin.async.dnn.blas.jblas;

import edu.snu.cay.dolphin.async.dnn.blas.Matrix;
import edu.snu.cay.dolphin.async.dnn.blas.MatrixFactory;
import org.apache.commons.math3.random.MersenneTwister;
import org.apache.commons.math3.random.SynchronizedRandomGenerator;
import org.jblas.FloatMatrix;

import javax.inject.Inject;

/**
 * Factory class for JBLAS based matrix implementation.
 */
public final class MatrixJBLASFactory implements MatrixFactory {

  private final SynchronizedRandomGenerator randomGenerator;

  @Inject
  private MatrixJBLASFactory() {
    this.randomGenerator = new SynchronizedRandomGenerator(new MersenneTwister());
  }

  @Override
  public Matrix create(final int length) {
    return new MatrixJBLASImpl(new FloatMatrix(length));
  }

  @Override
  public Matrix create(final int rows, final int columns) {
    return new MatrixJBLASImpl(new FloatMatrix(rows, columns));
  }

  @Override
  public Matrix create(final float[] data) {
    return new MatrixJBLASImpl(new FloatMatrix(data));
  }

  @Override
  public Matrix create(final float[][] data) {
    return new MatrixJBLASImpl(new FloatMatrix(data));
  }

  @Override
  public Matrix create(final float[] data, final int rows, final int columns) {
    return new MatrixJBLASImpl(new FloatMatrix(rows, columns, data));
  }

  @Override
  public Matrix ones(final int length) {
    return new MatrixJBLASImpl(FloatMatrix.ones(length));
  }

  @Override
  public Matrix ones(final int rows, final int columns) {
    return new MatrixJBLASImpl(FloatMatrix.ones(rows, columns));
  }

  @Override
  public Matrix zeros(final int length) {
    return new MatrixJBLASImpl(FloatMatrix.zeros(length));
  }

  @Override
  public Matrix zeros(final int rows, final int columns) {
    return new MatrixJBLASImpl(FloatMatrix.zeros(rows, columns));
  }

  @Override
  public void setRandomSeed(final long seed) {
    randomGenerator.setSeed(seed);
  }

  @Override
  public Matrix rand(final int length) {
    return rand(length, 1);
  }

  @Override
  public Matrix rand(final int rows, final int columns) {
    final int length = rows * columns;
    final float[] data = new float[length];

    for (int i = 0; i < length; ++i) {
      data[i] = randomGenerator.nextFloat();
    }

    return create(data, rows, columns);
  }

  @Override
  public Matrix rand(final int rows, final int columns, final long seed) {
    randomGenerator.setSeed(seed);
    return rand(rows, columns);
  }

  @Override
  public Matrix randn(final int length) {
    return randn(length, 1);
  }

  @Override
  public Matrix randn(final int rows, final int columns) {
    final int length = rows * columns;
    final float[] data = new float[length];

    for (int i = 0; i < length; ++i) {
      data[i] = (float) randomGenerator.nextGaussian();
    }

    return create(data, rows, columns);
  }

  @Override
  public Matrix randn(final int rows, final int columns, final long seed) {
    randomGenerator.setSeed(seed);
    return randn(rows, columns);
  }

  @Override
  public Matrix bernoulli(final int rows, final int columns, final float prob) {
    return bernoulli(rows, columns, prob, 1);
  }

  @Override
  public Matrix bernoulli(final int rows, final int columns, final float prob, final float scale) {
    final int length = rows * columns;
    final float[] data = new float[length];

    for (int i = 0; i < length; ++i) {
      if (randomGenerator.nextFloat() <= prob) {
        data[i] = scale;
      } else {
        data[i] = 0;
      }
    }
    return create(data, rows, columns);
  }

  @Override
  public Matrix concatHorizontally(final Matrix a, final Matrix b) {
    if (a instanceof MatrixJBLASImpl && b instanceof MatrixJBLASImpl) {
      return MatrixJBLASImpl.concatHorizontally((MatrixJBLASImpl) a, (MatrixJBLASImpl) b);
    }

    if (a.getRows() != b.getRows()) {
      throw new RuntimeException("Matrices do not have the same number of rows");
    } else {
      final Matrix ret = create(a.getRows(), a.getColumns() + b.getColumns());
      for (int i = 0; i < a.getColumns(); ++i) {
        ret.putColumn(i, a.getColumn(i));
      }
      for (int i = 0; i < b.getColumns(); ++i) {
        ret.putColumn(a.getColumns() + i, b.getColumn(i));
      }
      return ret;
    }
  }

  @Override
  public Matrix concatVertically(final Matrix a, final Matrix b) {
    if (a instanceof MatrixJBLASImpl && b instanceof MatrixJBLASImpl) {
      return MatrixJBLASImpl.concatVertically((MatrixJBLASImpl) a, (MatrixJBLASImpl) b);
    }

    if (a.getColumns() != b.getColumns()) {
      throw new RuntimeException("Matrices do not have the same number of columns");
    } else {
      final Matrix ret = create(a.getRows() + b.getRows(), a.getColumns());
      for (int i = 0; i < a.getRows(); ++i) {
        ret.putRow(i, a.getRow(i));
      }
      for (int i = 0; i < b.getRows(); ++i) {
        ret.putRow(a.getRows() + i, b.getRow(i));
      }
      return ret;
    }
  }
}
