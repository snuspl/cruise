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
package edu.snu.cay.dolphin.async.dnn.blas;

import edu.snu.cay.dolphin.async.dnn.blas.cuda.MatrixCudaImpl;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Utility class for {@link Matrix}.
 */
public final class MatrixUtils {

  private MatrixUtils() {
  }

  /**
   * Returns true if and only if two lists of Matrix have same size
   * and Matrix elements are equal to one another within tolerance.
   *
   * @param a one list of Matrix for comparison.
   * @param b another list of Matrix for comparison.
   * @param tolerance the maximum difference for which both numbers are still considered equal.
   * @return true if the two specified lists of Matrix are equal to one another.
   */
  public static boolean compare(final List<Matrix> a, final List<Matrix> b, final float tolerance) {
    if (a.size() != b.size()) {
      return false;
    }
    final Iterator<Matrix> bIter = b.iterator();
    for (final Matrix m : a) {
      if (!m.compare(bIter.next(), tolerance)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns true if and only if two arrays of Matrix have same size
   * and Matrix elements are equal to one another within tolerance.
   *
   * @param a one array of Matrix for comparison.
   * @param b another array of Matrix for comparison.
   * @param tolerance the maximum difference for which both numbers are still considered equal.
   * @return true if the two specified arrays of Matrix are equal to one another.
   */
  public static boolean compare(final Matrix[] a, final Matrix[] b, final float tolerance) {
    return compare(Arrays.asList(a), Arrays.asList(b), tolerance);
  }


  /**
   * Loads a matrix from an input stream of a Numpy-compatible plain text file with the specified delimiter.
   * @param matrixFactory a matrix factory used to create a matrix
   * @param inputStream a Numpy-compatible plain text input stream
   * @param delimiter a delimiter
   * @return a loaded matrix
   * @throws IOException
   */
  public static Matrix readNumpy(final MatrixFactory matrixFactory,
                                 final InputStream inputStream,
                                 final String delimiter) throws IOException {
    final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
    String line;
    final List<float[]> dataList = new ArrayList<>();
    int numRows = -1;
    final Matrix ret;
    while ((line = reader.readLine()) != null) {
      final String[] data = line.trim().split(delimiter);
      if (numRows < 0) {
        numRows = data.length;
      } else {
        if (data.length != numRows) {
          throw new RuntimeException("Data has inconsistent length");
        }
      }
      dataList.add(readSplit(data));
    }

    ret = matrixFactory.create(numRows, dataList.size());
    for (int i = 0; i < dataList.size(); i++) {
      ret.putColumn(i, matrixFactory.create(dataList.get(i)));
    }
    return ret;
  }

  private static float[] readSplit(final String[] split) {
    final float[] ret = new float[split.length];
    for (int i = 0; i < split.length; i++) {
      ret[i] = Float.parseFloat(split[i]);
    }
    return ret;
  }

  /**
   * Loads a matrix from a Numpy-compatible plain text file with the specified delimiter.
   * @param matrixFactory a matrix factory used to create a matrix
   * @param filePath a path for the Numpy-compatible plain text file
   * @param delimiter a delimiter
   * @return a loaded matrix
   * @throws IOException
   */
  public static Matrix readNumpy(final MatrixFactory matrixFactory,
                                 final String filePath,
                                 final String delimiter) throws IOException {
    return readNumpy(matrixFactory, new FileInputStream(filePath), delimiter);
  }

  /**
   * Creates a column vector in which the only element at the specified position is {@code 1}
   * and other elements are {@code 0}.
   *
   * @param matrixFactory a matrix factory used to create a matrix
   * @param index an index of the element to be set to {@code}
   * @param length the length of a column vector
   * @return a generated column vector
   */
  public static Matrix createOutputVector(final MatrixFactory matrixFactory, final int index, final int length) {
    return matrixFactory.zeros(length).put(index, 1.0f);
  }

  /**
   * Creates a matrix of which each column is a one-hot vector specified by each element of the given indices array.
   * @param matrixFactory a matrix factory used to create a matrix
   * @param indices the array of indices that indicate the one-hot positions
   * @param length the length of each column vector, in other words, the number of rows of the return matrix
   * @return the generated matrix
   */
  public static Matrix createOutputMatrix(final MatrixFactory matrixFactory, final int[] indices, final int length) {
    final Matrix ret = matrixFactory.zeros(length, indices.length);
    for (int i = 0; i < indices.length; ++i) {
      ret.put(indices[i], i, 1.0f);
    }
    return ret;
  }

  /**
<<<<<<< HEAD
   * Sets a matrix with each column is a one-hot vector specified by each element of the give indices array.
   * @param matrix a matrix to put data
   * @param indices the array of indices that indicate the one-hot positions
   * @param length the length of each column vector, in other words, the number of rows of the return matrix
   * @return the updated matrix
   */
  public static Matrix setOutputMatrix(final Matrix matrix, final int[] indices, final int length) {
    if (matrix.getRows() != length || matrix.getColumns() != indices.length) {
      throw new RuntimeException("matrix size is incorrect");
    }

    matrix.fill(0);
    for (int i = 0; i < indices.length; ++i) {
      matrix.put(indices[i], i, 1.0f);
    }
    return matrix;
  }

  /**
   * Destroy memory allocation of a matrix if and only if the matrix is an instance of {@link MatrixCudaImpl}.
   * @param matrix a matrix to destroy
   */
  public static void free(final Matrix matrix) {
    if (matrix != null && matrix instanceof MatrixCudaImpl) {
      ((MatrixCudaImpl)matrix).free();
    }
  }
}
