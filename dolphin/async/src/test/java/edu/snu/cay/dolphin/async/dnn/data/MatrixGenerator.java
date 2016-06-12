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

import java.util.Random;

/**
 * Utility class for generating random {@link edu.snu.cay.dolphin.async.dnn.blas.Matrix}.
 */
public final class MatrixGenerator {

  private MatrixGenerator() {
  }

  public static Matrix generateRandomVector(final MatrixFactory matrixFactory, final Random random) {
    return generateRandomMatrix(matrixFactory, random, 1, random.nextInt(100) + 100);
  }

  public static Matrix generateRandomMatrix(final MatrixFactory matrixFactory, final Random random) {
    return generateRandomMatrix(matrixFactory, random, random.nextInt(100) + 100, random.nextInt(100) + 100);
  }

  public static Matrix generateRandomMatrix(final MatrixFactory matrixFactory,
                                            final Random random, final int rows, final int columns) {
    final Matrix ret = matrixFactory.create(rows, columns);

    for (int i = 0; i < ret.getLength(); ++i) {
      ret.put(i, random.nextFloat());
    }

    return ret;
  }
}
