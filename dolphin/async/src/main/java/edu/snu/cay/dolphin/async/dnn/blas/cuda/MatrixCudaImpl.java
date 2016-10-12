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
import org.bytedeco.javacpp.FloatPointer;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * CUDA backend matrix implementation.
 */
public final class MatrixCudaImpl implements Matrix {

  static final int FLOAT_SIZE = 4; // size of float in bytes

  private int rows;
  private int columns;
  private int length;
  private FloatPointer devPtr;
  private int multiplierLength;
  private FloatPointer multiplierPtr = new FloatPointer(); // max(rows, columns)-length device memory filled with one.
  // allocated if necessary

  private void init(final int newRows, final int newColumns) {
    this.rows = newRows;
    this.columns = newColumns;
    this.length = newRows * newColumns;
    this.devPtr = new FloatPointer(JavaCuda.deviceMalloc(FLOAT_SIZE * length));
  }

  private void initMultiplier() {
    this.multiplierLength = Math.max(rows, columns);
    this.multiplierPtr = new FloatPointer(JavaCuda.deviceMalloc(FLOAT_SIZE * multiplierLength));
    if (!JavaCuda.set(multiplierPtr, 1.0f, multiplierLength)) {
      throw new RuntimeException("Failed to set multiplier memory");
    }
  }

  MatrixCudaImpl(final int rows, final int columns) {
    init(rows, columns);
  }

  MatrixCudaImpl(final float[] data) {
    this(data.length, 1);
    copyToDevice(data);
  }

  MatrixCudaImpl(final float[][] data) {
    this(data.length, data[0].length);
    copyToDevice(toColumnMajor1DArray(data));
  }

  MatrixCudaImpl(final float[] data, final int rows, final int columns) {
    this(rows, columns);
    copyToDevice(data);
  }

  /**
   * Copy the float array to the GPU device.
   * @param data the float array to copy
   */
  private void copyToDevice(final float[] data) {
    final FloatPointer hostPtr = new FloatPointer(data);
    if (!JavaCuda.h2dMemcpy(devPtr, hostPtr, FLOAT_SIZE * data.length)) {
      throw new RuntimeException("Failed to copy memory to GPU");
    }
  }

  /**
   * Transform the 2-D float array into the 1-D float array in column-major order.
   * @param data the 2-D float array to transform
   * @return the 1-D float array in column-major order
   */
  private static float[] toColumnMajor1DArray(final float[][] data) {
    final int rows = data.length;
    if (rows == 0) {
      throw new IllegalArgumentException("The 1-D length in the 2-D array should be greater than 0.");
    }
    final int columns = data[0].length;
    final float[] retVal = new float[rows * columns];
    int dstIdx = 0;
    for (int j = 0; j < columns; ++j) {
      for (int i = 0; i < rows; ++i) {
        retVal[dstIdx++] = data[i][j];
      }
    }
    return retVal;
  }

  @Override
  public int getRows() {
    return rows;
  }

  @Override
  public int getColumns() {
    return columns;
  }

  @Override
  public float get(final int index) {
    final FloatPointer hostPtr = new FloatPointer(1);
    if (!JavaCuda.d2hMemcpy(hostPtr, devPtr.position(FLOAT_SIZE * index), FLOAT_SIZE)) {
      devPtr.position(0);
      throw new RuntimeException("Failed to copy memory from GPU.");
    }
    devPtr.position(0);
    return hostPtr.get();
  }

  @Override
  public Matrix get(final int[] indices) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public float get(final int rowIndex, final int columnIndex) {
    final FloatPointer hostPtr = new FloatPointer(1);
    if (!JavaCuda.d2hMemcpy(hostPtr, devPtr.position(FLOAT_SIZE * (rowIndex + columnIndex * rows)), FLOAT_SIZE)) {
      devPtr.position(0);
      throw new RuntimeException("Failed to copy memory from GPU.");
    }
    devPtr.position(0);
    return hostPtr.get();
  }

  @Override
  public Matrix put(final int index, final float value) {
    final FloatPointer hostPtr = new FloatPointer(1);
    hostPtr.put(value);
    if (!JavaCuda.h2dMemcpy(devPtr.position(FLOAT_SIZE * index), hostPtr, FLOAT_SIZE)) {
      devPtr.position(0);
      throw new RuntimeException("Failed to copy memory from GPU.");
    }
    devPtr.position(0);
    return this;
  }

  @Override
  public Matrix put(final int rowIndex, final int columnIndex, final float value) {
    final FloatPointer hostPtr = new FloatPointer(1);
    hostPtr.put(value);
    if (!JavaCuda.h2dMemcpy(devPtr.position(FLOAT_SIZE * (rowIndex + columnIndex * rows)), hostPtr, FLOAT_SIZE)) {
      devPtr.position(0);
      throw new RuntimeException("Failed to copy memory from GPU.");
    }
    devPtr.position(0);
    return this;
  }

  @Override
  public void putColumn(final int index, final Matrix vector) {
    if (index < 0 || index >= columns) {
      throw new IndexOutOfBoundsException(String.format(
          "The index should be greater than zero and less than the number of columns(%d): %d", columns, index));
    }
    if (!vector.isColumnVector()) {
      throw new IllegalArgumentException("The argument should be a column vector");
    }
    if (vector.getLength() != rows) {
      throw new IllegalArgumentException("The length of the given vector should be " + rows);
    }
    final FloatPointer otherDevPtr = toCudaImpl(vector).getDevicePointer();
    if (!JavaCuda.d2dMemcpy(devPtr.position(FLOAT_SIZE * index * rows), otherDevPtr, FLOAT_SIZE * rows)) {
      devPtr.position(0);
      throw new RuntimeException("Failed to copy GPU memory");
    }
    devPtr.position(0);
  }

  @Override
  public void putRow(final int index, final Matrix vector) {
    if (index < 0 || index >= rows) {
      throw new IndexOutOfBoundsException(
          String.format("The index should be greater than zero and less than the number of rows(%d): %d", rows, index));
    }
    if (!vector.isRowVector()) {
      throw new IllegalArgumentException("The argument should be a row vector");
    }
    if (vector.getLength() != columns) {
      throw new IllegalArgumentException("The length of the given vector should be " + columns);
    }
    final FloatPointer otherDevPtr = toCudaImpl(vector).getDevicePointer();
    if (!JavaCuda.copy(columns, otherDevPtr, 1, devPtr.position(index), rows)) {
      devPtr.position(0);
      throw new RuntimeException("Failed to copy GPU memory");
    }
    devPtr.position(0);
  }

  @Override
  public Matrix getColumn(final int index) {
    if (index < 0 || index >= columns) {
      throw new IndexOutOfBoundsException(String.format(
          "The index should be greater than zero and less than the number of columns(%d): %d", columns, index));
    }
    final MatrixCudaImpl newMatrix = new MatrixCudaImpl(rows, 1);
    if (!JavaCuda.d2dMemcpy(
        newMatrix.getDevicePointer(), devPtr.position(FLOAT_SIZE * index * rows), FLOAT_SIZE * rows)) {
      devPtr.position(0);
      newMatrix.free();
      throw new RuntimeException("Failed to copy GPU memory");
    }
    devPtr.position(0);
    return newMatrix;
  }

  @Override
  public Matrix getRow(final int index) {
    if (index < 0 || index >= rows) {
      throw new IndexOutOfBoundsException(
          String.format("The index should be greater than zero and less than the number of rows(%d): %d", rows, index));
    }
    final MatrixCudaImpl newMatrix = new MatrixCudaImpl(1, columns);
    if (!JavaCuda.copy(columns, devPtr.position(index), rows, newMatrix.getDevicePointer(), 1)) {
      devPtr.position(0);
      newMatrix.free();
      throw new RuntimeException("Failed to copy GPU memory");
    }
    devPtr.position(0);
    return newMatrix;
  }

  @Override
  public int getLength() {
    return length;
  }

  @Override
  public boolean isColumnVector() {
    return columns == 1;
  }

  @Override
  public boolean isRowVector() {
    return rows == 1;
  }

  @Override
  public Matrix fill(final float value) {
    if (!JavaCuda.set(devPtr, value, length)) {
      throw new RuntimeException("Failed to fill matrix.");
    }
    return this;
  }

  @Override
  public Matrix reshape(final int newRows, final int newColumns) {
    if (newRows * newColumns != length) {
      throw new IllegalArgumentException(String.format(
          "The total number of element should not change. %d x %d != %d", newRows, newColumns, length));
    }
    this.rows = newRows;
    this.columns = newColumns;
    if (!multiplierPtr.isNull() && Math.max(rows, columns) != multiplierLength) {
      multiplierFree();
    }
    return this;
  }

  @Override
  public String toString() {
    final StringWriter s = new StringWriter();
    final PrintWriter p = new PrintWriter(s);
    final float[] data = toFloatArray();

    for (int r = 0; r < this.rows; ++r) {
      for (int c = 0; c < this.columns; ++c) {
        p.printf("%f", data[c * rows + r]);
        if (c < this.columns - 1) {
          p.print(", ");
        }
      }

      if (r < this.rows - 1) {
        p.print("\n");
      }
    }

    return s.toString();
  }

  @Override
  public float[] toFloatArray() {
    final FloatPointer hostPtr = new FloatPointer(length);
    final float[] retVal = new float[length];
    if (!JavaCuda.d2hMemcpy(hostPtr, devPtr, FLOAT_SIZE * length)) {
      throw new RuntimeException("Failed to generate float array. An error occurred when copying memory from GPU.");
    }
    hostPtr.get(retVal);
    return retVal;
  }

  @Override
  public Matrix copy(final Matrix matrix) {
    final MatrixCudaImpl other = toCudaImpl(matrix);
    if (length != other.getLength()) {
      // allocate new memory
      deviceFree();
      init(other.getRows(), other.getColumns());
      if (!multiplierPtr.isNull() && Math.max(rows, columns) != multiplierLength) {
        multiplierFree();
      }
    } else {
      this.rows = other.getRows();
      this.columns = other.getColumns();
      if (!multiplierPtr.isNull() && Math.max(rows, columns) != multiplierLength) {
        multiplierFree();
      }
    }
    if (!JavaCuda.d2dMemcpy(devPtr, other.getDevicePointer(), FLOAT_SIZE * length)) {
      throw new RuntimeException("Failed to copy GPU memory");
    }
    return this;
  }

  @Override
  public MatrixCudaImpl dup() {
    final MatrixCudaImpl newMatrix = new MatrixCudaImpl(rows, columns);
    if (!JavaCuda.d2dMemcpy(newMatrix.getDevicePointer(), devPtr, FLOAT_SIZE * length)) {
      newMatrix.free();
      throw new RuntimeException("Failed to copy memory");
    }
    return newMatrix;
  }

  @Override
  public Matrix transpose() {
    final MatrixCudaImpl newMatrix = new MatrixCudaImpl(columns, rows);
    if (!JavaCuda.geam('T', 'N', columns, rows, 1.0f, devPtr, rows, 0.0f,
        newMatrix.getDevicePointer(), columns, newMatrix.getDevicePointer(), columns)) {
      newMatrix.free();
      throw new RuntimeException("Failed to transpose matrix");
    }
    return newMatrix;
  }

  @Override
  public Matrix add(final float value) {
    final MatrixCudaImpl newMatrix = new MatrixCudaImpl(rows, columns);
    if (!JavaCuda.addScalar(length, value, devPtr, newMatrix.devPtr)) {
      newMatrix.free();
      throw new RuntimeException("Failed to add a scalar value");
    }
    return newMatrix;
  }

  @Override
  public Matrix addi(final float value) {
    if (!JavaCuda.addScalar(length, value, devPtr, devPtr)) {
      throw new RuntimeException("Failed to add a scalar value");
    }
    return this;
  }

  @Override
  public Matrix add(final Matrix matrix) {
    checkElementWiseOpValidity(matrix);
    final MatrixCudaImpl other = toCudaImpl(matrix);
    final MatrixCudaImpl newMatrix = new MatrixCudaImpl(rows, columns);
    if (!JavaCuda.add(length, devPtr, other.getDevicePointer(), newMatrix.getDevicePointer())) {
      newMatrix.free();
      throw new RuntimeException("Failed to perform element-wise addition");
    }
    return newMatrix;
  }

  @Override
  public Matrix addi(final Matrix matrix) {
    checkElementWiseOpValidity(matrix);
    final MatrixCudaImpl other = toCudaImpl(matrix);
    if (!JavaCuda.add(length, devPtr, other.getDevicePointer(), devPtr)) {
      throw new RuntimeException("Failed to perform element-wise addition");
    }
    return this;
  }

  @Override
  public Matrix addColumnVector(final Matrix vector) {
    if (!vector.isColumnVector()) {
      throw new IllegalArgumentException("The argument should be a column vector");
    }
    if (vector.getLength() != rows) {
      throw new IllegalArgumentException("The length of the given vector should be " + rows);
    }
    final MatrixCudaImpl other = toCudaImpl(vector);
    if (multiplierPtr.isNull()) {
      initMultiplier();
    }
    final MatrixCudaImpl newMatrix = dup();
    if (!JavaCuda.gemm('N', 'N',
        rows, columns, 1, 1.0f, other.devPtr, rows, multiplierPtr, 1, 1.0f, newMatrix.devPtr, rows)) {
      throw new RuntimeException("Failed to perform matrix-matrix multiplication");
    }
    return newMatrix;
  }

  @Override
  public Matrix addiColumnVector(final Matrix vector) {
    if (!vector.isColumnVector()) {
      throw new IllegalArgumentException("The argument should be a column vector");
    }
    if (vector.getLength() != rows) {
      throw new IllegalArgumentException("The length of the given vector should be " + rows);
    }
    final MatrixCudaImpl other = toCudaImpl(vector);
    if (multiplierPtr.isNull()) {
      initMultiplier();
    }
    if (!JavaCuda.gemm('N', 'N', rows, columns, 1, 1.0f, other.devPtr, rows, multiplierPtr, 1, 1.0f, devPtr, rows)) {
      throw new RuntimeException("Failed to perform matrix-matrix multiplication");
    }
    return this;
  }

  @Override
  public Matrix addRowVector(final Matrix vector) {
    if (!vector.isRowVector()) {
      throw new IllegalArgumentException("The argument should be a row vector");
    }
    if (vector.getLength() != columns) {
      throw new IllegalArgumentException("The length of the given vector should be " + columns);
    }
    final MatrixCudaImpl other = toCudaImpl(vector);
    if (multiplierPtr.isNull()) {
      initMultiplier();
    }
    final MatrixCudaImpl newMatrix = dup();
    if (!JavaCuda.gemm('N', 'N',
        rows, columns, 1, 1.0f, multiplierPtr, rows, other.devPtr, 1, 1.0f, newMatrix.devPtr, rows)) {
      throw new RuntimeException("Failed to perform matrix-matrix multiplication");
    }
    return newMatrix;
  }

  @Override
  public Matrix addiRowVector(final Matrix vector) {
    if (!vector.isRowVector()) {
      throw new IllegalArgumentException("The argument should be a row vector");
    }
    if (vector.getLength() != columns) {
      throw new IllegalArgumentException("The length of the given vector should be " + columns);
    }
    final MatrixCudaImpl other = toCudaImpl(vector);
    if (multiplierPtr.isNull()) {
      initMultiplier();
    }
    if (!JavaCuda.gemm('N', 'N',
        rows, columns, 1, 1.0f, multiplierPtr, rows, other.devPtr, 1, 1.0f, devPtr, rows)) {
      throw new RuntimeException("Failed to perform matrix-matrix multiplication");
    }
    return this;
  }

  @Override
  public Matrix sub(final float value) {
    final MatrixCudaImpl newMatrix = new MatrixCudaImpl(rows, columns);
    if (!JavaCuda.addScalar(length, -value, devPtr, newMatrix.devPtr)) {
      newMatrix.free();
      throw new RuntimeException("Failed to add a scalar value");
    }
    return newMatrix;
  }

  @Override
  public Matrix subi(final float value) {
    if (!JavaCuda.addScalar(length, -value, devPtr, devPtr)) {
      throw new RuntimeException("Failed to subtract a scalar value");
    }
    return this;
  }

  @Override
  public Matrix sub(final Matrix matrix) {
    final MatrixCudaImpl newMatrix = new MatrixCudaImpl(rows, columns);
    try {
      return sub(matrix, newMatrix);
    } catch (Exception e) {
      newMatrix.free();
      throw e;
    }
  }

  @Override
  public Matrix subi(final Matrix matrix) {
    return sub(matrix, this);
  }

  @Override
  public Matrix sub(final Matrix matrix, final Matrix result) {
    checkElementWiseOpValidity(matrix);
    checkElementWiseOpValidity(result);
    final MatrixCudaImpl other = toCudaImpl(matrix);
    final MatrixCudaImpl resultMatrix = toCudaImpl(result);
    if (!JavaCuda.sub(length, devPtr, other.getDevicePointer(), resultMatrix.getDevicePointer())) {
      throw new RuntimeException("Failed to perform element-wise subtraction");
    }
    return result;
  }

  @Override
  public Matrix subColumnVector(final Matrix vector) {
    if (!vector.isColumnVector()) {
      throw new IllegalArgumentException("The argument should be a column vector");
    }
    if (vector.getLength() != rows) {
      throw new IllegalArgumentException("The length of the given vector should be " + rows);
    }
    final MatrixCudaImpl other = toCudaImpl(vector);
    if (multiplierPtr.isNull()) {
      initMultiplier();
    }
    final MatrixCudaImpl newMatrix = dup();
    if (!JavaCuda.gemm('N', 'N',
        rows, columns, 1, -1.0f, other.devPtr, rows, multiplierPtr, 1, 1.0f, newMatrix.devPtr, rows)) {
      throw new RuntimeException("Failed to perform matrix-matrix multiplication");
    }
    return newMatrix;
  }

  @Override
  public Matrix subiColumnVector(final Matrix vector) {
    if (!vector.isColumnVector()) {
      throw new IllegalArgumentException("The argument should be a column vector");
    }
    if (vector.getLength() != rows) {
      throw new IllegalArgumentException("The length of the given vector should be " + rows);
    }
    final MatrixCudaImpl other = toCudaImpl(vector);
    if (multiplierPtr.isNull()) {
      initMultiplier();
    }
    if (!JavaCuda.gemm('N', 'N', rows, columns, 1, -1.0f, other.devPtr, rows, multiplierPtr, 1, 1.0f, devPtr, rows)) {
      throw new RuntimeException("Failed to perform matrix-matrix multiplication");
    }
    return this;
  }

  @Override
  public Matrix subRowVector(final Matrix vector) {
    if (!vector.isRowVector()) {
      throw new IllegalArgumentException("The argument should be a row vector");
    }
    if (vector.getLength() != columns) {
      throw new IllegalArgumentException("The length of the given vector should be " + columns);
    }
    final MatrixCudaImpl other = toCudaImpl(vector);
    if (multiplierPtr.isNull()) {
      initMultiplier();
    }
    final MatrixCudaImpl newMatrix = dup();
    if (!JavaCuda.gemm('N', 'N',
        rows, columns, 1, -1.0f, multiplierPtr, rows, other.devPtr, 1, 1.0f, newMatrix.devPtr, rows)) {
      throw new RuntimeException("Failed to perform matrix-matrix multiplication");
    }
    return newMatrix;
  }

  @Override
  public Matrix subiRowVector(final Matrix vector) {
    if (!vector.isRowVector()) {
      throw new IllegalArgumentException("The argument should be a row vector");
    }
    if (vector.getLength() != columns) {
      throw new IllegalArgumentException("The length of the given vector should be " + columns);
    }
    final MatrixCudaImpl other = toCudaImpl(vector);
    if (multiplierPtr.isNull()) {
      initMultiplier();
    }
    if (!JavaCuda.gemm('N', 'N', rows, columns, 1, -1.0f, multiplierPtr, rows, other.devPtr, 1, 1.0f, devPtr, rows)) {
      throw new RuntimeException("Failed to perform matrix-matrix multiplication");
    }
    return this;
  }

  @Override
  public Matrix rsub(final float value) {
    final MatrixCudaImpl newMatrix = new MatrixCudaImpl(rows, columns);
    if (!JavaCuda.rsubScalar(length, value, devPtr, newMatrix.devPtr)) {
      newMatrix.free();
      throw new RuntimeException("Failed to add a scalar value");
    }
    return newMatrix;
  }

  @Override
  public Matrix rsubi(final float value) {
    if (!JavaCuda.rsubScalar(length, value, devPtr, devPtr)) {
      throw new RuntimeException("Failed to perform element-wise right division");
    }
    return this;
  }

  @Override
  public Matrix rsub(final Matrix matrix) {
    checkElementWiseOpValidity(matrix);
    final MatrixCudaImpl other = toCudaImpl(matrix);
    final MatrixCudaImpl newMatrix = new MatrixCudaImpl(rows, columns);
    if (!JavaCuda.sub(length, other.getDevicePointer(), devPtr, newMatrix.getDevicePointer())) {
      newMatrix.free();
      throw new RuntimeException("Failed to perform element-wise subtraction");
    }
    return newMatrix;
  }

  @Override
  public Matrix rsubi(final Matrix matrix) {
    checkElementWiseOpValidity(matrix);
    final MatrixCudaImpl other = toCudaImpl(matrix);
    if (!JavaCuda.sub(length, other.getDevicePointer(), devPtr, devPtr)) {
      throw new RuntimeException("Failed to perform element-wise right subtraction");
    }
    return this;
  }

  @Override
  public Matrix mul(final float value) {
    final MatrixCudaImpl newMatrix = new MatrixCudaImpl(rows, columns);
    if (!JavaCuda.mulScalar(length, value, devPtr, newMatrix.devPtr)) {
      newMatrix.free();
      throw new RuntimeException("Failed to add a scalar value");
    }
    return newMatrix;
  }

  @Override
  public Matrix muli(final float value) {
    if (!JavaCuda.mulScalar(length, value, devPtr, devPtr)) {
      throw new RuntimeException("Failed to multiply a scalar value");
    }
    return this;
  }

  @Override
  public Matrix mul(final Matrix matrix) {
    checkElementWiseOpValidity(matrix);
    final MatrixCudaImpl other = toCudaImpl(matrix);
    final MatrixCudaImpl newMatrix = new MatrixCudaImpl(rows, columns);
    if (!JavaCuda.mul(length, devPtr, other.getDevicePointer(), newMatrix.getDevicePointer())) {
      newMatrix.free();
      throw new RuntimeException("Failed to perform element-wise multiplication");
    }
    return newMatrix;
  }

  @Override
  public Matrix mul(final Matrix matrix, final Matrix result) {
    checkElementWiseOpValidity(matrix);
    checkElementWiseOpValidity(result);

    final MatrixCudaImpl other = toCudaImpl(matrix);
    final MatrixCudaImpl resultMatrix = toCudaImpl(result);
    if (!JavaCuda.mul(length, devPtr, other.getDevicePointer(), resultMatrix.getDevicePointer())) {
      throw new RuntimeException("Failed to perform element-wise multiplication");
    }
    return resultMatrix;
  }

  @Override
  public Matrix muli(final Matrix matrix) {
    checkElementWiseOpValidity(matrix);
    final MatrixCudaImpl other = toCudaImpl(matrix);
    if (!JavaCuda.mul(length, devPtr, other.getDevicePointer(), devPtr)) {
      throw new RuntimeException("Failed to perform element-wise multiplication");
    }
    return this;
  }

  @Override
  public Matrix mulColumnVector(final Matrix vector) {
    if (!vector.isColumnVector()) {
      throw new IllegalArgumentException("The argument should be a column vector");
    }
    if (vector.getLength() != rows) {
      throw new IllegalArgumentException("The length of the given vector should be " + rows);
    }
    final MatrixCudaImpl other = toCudaImpl(vector);
    final MatrixCudaImpl newMatrix = new MatrixCudaImpl(rows, columns);
    if (!JavaCuda.mulColumnVector(rows, columns, other.devPtr, devPtr, newMatrix.devPtr)) {
      newMatrix.free();
      throw new RuntimeException("Failed to perform element-wise multiplication");
    }
    return newMatrix;
  }

  @Override
  public Matrix muliColumnVector(final Matrix vector) {
    if (!vector.isColumnVector()) {
      throw new IllegalArgumentException("The argument should be a column vector");
    }
    if (vector.getLength() != rows) {
      throw new IllegalArgumentException("The length of the given vector should be " + rows);
    }
    final MatrixCudaImpl other = toCudaImpl(vector);
    if (!JavaCuda.mulColumnVector(rows, columns, other.devPtr, devPtr, devPtr)) {
      throw new RuntimeException("Failed to perform element-wise multiplication");
    }
    return this;
  }

  @Override
  public Matrix mulRowVector(final Matrix vector) {
    if (!vector.isRowVector()) {
      throw new IllegalArgumentException("The argument should be a row vector");
    }
    if (vector.getLength() != columns) {
      throw new IllegalArgumentException("The length of the given vector should be " + columns);
    }
    final MatrixCudaImpl other = toCudaImpl(vector);
    final MatrixCudaImpl newMatrix = new MatrixCudaImpl(rows, columns);
    if (!JavaCuda.mulRowVector(rows, columns, other.devPtr, devPtr, newMatrix.devPtr)) {
      newMatrix.free();
      throw new RuntimeException("Failed to perform element-wise division");
    }
    return newMatrix;
  }

  @Override
  public Matrix muliRowVector(final Matrix vector) {
    if (!vector.isRowVector()) {
      throw new IllegalArgumentException("The argument should be a row vector");
    }
    if (vector.getLength() != columns) {
      throw new IllegalArgumentException("The length of the given vector should be " + columns);
    }
    final MatrixCudaImpl other = toCudaImpl(vector);
    if (!JavaCuda.mulRowVector(rows, columns, other.devPtr, devPtr, devPtr)) {
      throw new RuntimeException("Failed to perform element-wise division");
    }
    return this;
  }

  @Override
  public Matrix div(final float value) {
    final MatrixCudaImpl newMatrix = new MatrixCudaImpl(rows, columns);
    if (!JavaCuda.mulScalar(length, 1 / value, devPtr, newMatrix.devPtr)) {
      newMatrix.free();
      throw new RuntimeException("Failed to add a scalar value");
    }
    return newMatrix;
  }

  @Override
  public Matrix divi(final float value) {
    if (!JavaCuda.mulScalar(length, 1 / value, devPtr, devPtr)) {
      throw new RuntimeException("Failed to multiply a scalar value");
    }
    return this;
  }

  @Override
  public Matrix div(final Matrix matrix) {
    checkElementWiseOpValidity(matrix);
    final MatrixCudaImpl other = toCudaImpl(matrix);
    final MatrixCudaImpl newMatrix = new MatrixCudaImpl(rows, columns);
    if (!JavaCuda.div(length, devPtr, other.getDevicePointer(), newMatrix.getDevicePointer())) {
      newMatrix.free();
      throw new RuntimeException("Failed to perform element-wise division");
    }
    return newMatrix;
  }

  @Override
  public Matrix divi(final Matrix matrix) {
    checkElementWiseOpValidity(matrix);
    final MatrixCudaImpl other = toCudaImpl(matrix);
    if (!JavaCuda.div(length, devPtr, other.getDevicePointer(), devPtr)) {
      throw new RuntimeException("Failed to perform element-wise division");
    }
    return this;
  }

  @Override
  public Matrix divColumnVector(final Matrix vector) {
    if (!vector.isColumnVector()) {
      throw new IllegalArgumentException("The argument should be a column vector");
    }
    if (vector.getLength() != rows) {
      throw new IllegalArgumentException("The length of the given vector should be " + rows);
    }
    final MatrixCudaImpl other = toCudaImpl(vector);
    final MatrixCudaImpl newMatrix = new MatrixCudaImpl(rows, columns);
    if (!JavaCuda.divColumnVector(rows, columns, other.devPtr, devPtr, newMatrix.devPtr)) {
      newMatrix.free();
      throw new RuntimeException("Failed to perform element-wise multiplication");
    }
    return newMatrix;
  }

  @Override
  public Matrix diviColumnVector(final Matrix vector) {
    if (!vector.isColumnVector()) {
      throw new IllegalArgumentException("The argument should be a column vector");
    }
    if (vector.getLength() != rows) {
      throw new IllegalArgumentException("The length of the given vector should be " + rows);
    }
    final MatrixCudaImpl other = toCudaImpl(vector);
    if (!JavaCuda.divColumnVector(rows, columns, other.devPtr, devPtr, devPtr)) {
      throw new RuntimeException("Failed to perform element-wise multiplication");
    }
    return this;
  }

  @Override
  public Matrix divRowVector(final Matrix vector) {
    if (!vector.isRowVector()) {
      throw new IllegalArgumentException("The argument should be a row vector");
    }
    if (vector.getLength() != columns) {
      throw new IllegalArgumentException("The length of the given vector should be " + columns);
    }
    final MatrixCudaImpl other = toCudaImpl(vector);
    final MatrixCudaImpl newMatrix = new MatrixCudaImpl(rows, columns);
    if (!JavaCuda.divRowVector(rows, columns, other.devPtr, devPtr, newMatrix.devPtr)) {
      newMatrix.free();
      throw new RuntimeException("Failed to perform element-wise division");
    }
    return newMatrix;
  }

  @Override
  public Matrix diviRowVector(final Matrix vector) {
    if (!vector.isRowVector()) {
      throw new IllegalArgumentException("The argument should be a row vector");
    }
    if (vector.getLength() != columns) {
      throw new IllegalArgumentException("The length of the given vector should be " + columns);
    }
    final MatrixCudaImpl other = toCudaImpl(vector);
    if (!JavaCuda.divRowVector(rows, columns, other.devPtr, devPtr, devPtr)) {
      throw new RuntimeException("Failed to perform element-wise division");
    }
    return this;
  }

  @Override
  public Matrix rdiv(final float value) {
    final MatrixCudaImpl newMatrix = new MatrixCudaImpl(rows, columns);
    if (!JavaCuda.rdivScalar(length, value, devPtr, newMatrix.devPtr)) {
      newMatrix.free();
      throw new RuntimeException("Failed to add a scalar value");
    }
    return newMatrix;
  }

  @Override
  public Matrix rdivi(final float value) {
    if (!JavaCuda.rdivScalar(length, value, devPtr, devPtr)) {
      throw new RuntimeException("Failed to perform element-wise right division");
    }
    return this;
  }

  @Override
  public Matrix rdiv(final Matrix matrix) {
    checkElementWiseOpValidity(matrix);
    final MatrixCudaImpl other = toCudaImpl(matrix);
    final MatrixCudaImpl newMatrix = new MatrixCudaImpl(rows, columns);
    if (!JavaCuda.div(length, other.getDevicePointer(), devPtr, newMatrix.getDevicePointer())) {
      newMatrix.free();
      throw new RuntimeException("Failed to perform element-wise right division");
    }
    return newMatrix;
  }

  @Override
  public Matrix rdivi(final Matrix matrix) {
    checkElementWiseOpValidity(matrix);
    final MatrixCudaImpl other = toCudaImpl(matrix);
    if (!JavaCuda.div(length, other.getDevicePointer(), devPtr, devPtr)) {
      throw new RuntimeException("Failed to perform element-wise right division");
    }
    return this;
  }

  @Override
  public Matrix mmul(final Matrix matrix) {
    final MatrixCudaImpl newMatrix = new MatrixCudaImpl(rows, matrix.getColumns());
    try {
      mmul(matrix, newMatrix);
      return newMatrix;
    } catch (Exception e) {
      newMatrix.free();
      throw e;
    }
  }

  @Override
  public Matrix mmuli(final Matrix matrix) {
    return mmul(matrix, this);
  }

  @Override
  public Matrix mmul(final Matrix matrix, final Matrix result) {

    if (columns != matrix.getRows()) {
      throw new IllegalArgumentException(
          "The number of columns of left matrix should be equal to the number of rows of right matrix.");
    }

    if (result.getRows() != rows || result.getColumns() != matrix.getColumns()) {
      throw new IllegalArgumentException("The size of result matrix is wrong");
    }

    final MatrixCudaImpl other = toCudaImpl(matrix);
    final MatrixCudaImpl resultMatrix = toCudaImpl(result);
    if (other.getColumns() == 1) {
      // column vector
      if (!JavaCuda.gemv('N',
          rows, columns, 1.0f, devPtr, rows, other.getDevicePointer(), 1, 0.0f, resultMatrix.getDevicePointer(), 1)) {
        throw new RuntimeException("Failed to perform matrix-vector multiplication");
      }
    } else {
      if (!JavaCuda.gemm('N', 'N', rows, other.getColumns(), columns, 1.0f,
          devPtr, rows, other.getDevicePointer(), columns, 0.0f, resultMatrix.getDevicePointer(), rows)) {
        throw new RuntimeException("Failed to perform matrix-matrix multiplication");
      }
    }
    return result;
  }

  @Override
  public Matrix tmmul(final Matrix matrix) {
    final MatrixCudaImpl newMatrix = new MatrixCudaImpl(columns, matrix.getColumns());
    try {
      tmmul(matrix, newMatrix);
      return newMatrix;
    } catch (Exception e) {
      newMatrix.free();
      throw e;
    }
  }

  @Override
  public Matrix tmmul(final Matrix matrix, final Matrix result) {

    if (rows != matrix.getRows()) {
      throw new IllegalArgumentException(
          "The number of rows of left matrix should be equal to the number of rows of right matrix.");
    }

    if (result.getRows() != columns || result.getColumns() != matrix.getColumns()) {
      throw new IllegalArgumentException("The size of result matrix is wrong");
    }

    final MatrixCudaImpl other = toCudaImpl(matrix);
    final MatrixCudaImpl resultMatrix = toCudaImpl(result);
    if (other.getColumns() == 1) {
      // column vector
      if (!JavaCuda.gemv('T',
          rows, columns, 1.0f, devPtr, rows, other.getDevicePointer(), 1, 0.0f, resultMatrix.getDevicePointer(), 1)) {
        throw new RuntimeException("Failed to perform matrix-vector multiplication");
      }
    } else {
      if (!JavaCuda.gemm('T', 'N', columns, other.getColumns(), rows, 1.0f,
          devPtr, rows, other.getDevicePointer(), other.getRows(), 0.0f, resultMatrix.getDevicePointer(), columns)) {
        throw new RuntimeException("Failed to perform matrix-matrix multiplication");
      }
    }
    return result;
  }

  @Override
  public Matrix mmult(final Matrix matrix) {
    final MatrixCudaImpl newMatrix = new MatrixCudaImpl(rows, matrix.getRows());
    try {
      mmult(matrix, newMatrix);
      return newMatrix;
    } catch (Exception e) {
      newMatrix.free();
      throw e;
    }
  }

  @Override
  public Matrix mmult(final Matrix matrix, final Matrix result) {
    if (columns != matrix.getColumns()) {
      throw new IllegalArgumentException(
          "The number of columns of left matrix should be equal to the number of columns of right matrix.");
    }

    if (result.getRows() != rows || result.getColumns() != matrix.getRows()) {
      throw new IllegalArgumentException("The size of result matrix is wrong");
    }

    final MatrixCudaImpl other = toCudaImpl(matrix);
    final MatrixCudaImpl resultMatrix = toCudaImpl(result);
    if (!JavaCuda.gemm('N', 'T', rows, other.getRows(), columns, 1.0f,
        devPtr, rows, other.getDevicePointer(), other.getRows(), 0.0f, resultMatrix.getDevicePointer(), rows)) {
      throw new RuntimeException("Failed to perform matrix-matrix multiplication");
    }
    return resultMatrix;
  }

  @Override
  public float max() {
    return JavaCuda.max(length, devPtr);
  }

  @Override
  public Matrix columnMaxs() {
    final MatrixCudaImpl newMatrix = new MatrixCudaImpl(1, columns);
    if (!JavaCuda.columnMax(rows, columns, devPtr, newMatrix.devPtr)) {
      newMatrix.free();
      throw new RuntimeException("Failed to get columnMaxs");
    }
    return newMatrix;
  }

  @Override
  public Matrix rowMaxs() {
    final MatrixCudaImpl newMatrix = new MatrixCudaImpl(rows, 1);
    if (!JavaCuda.rowMax(rows, columns, devPtr, newMatrix.devPtr)) {
      newMatrix.free();
      throw new RuntimeException("Failed to get rowMaxs");
    }
    return newMatrix;
  }

  @Override
  public float min() {
    return JavaCuda.min(length, devPtr);
  }

  @Override
  public Matrix columnMins() {
    final MatrixCudaImpl newMatrix = new MatrixCudaImpl(1, columns);
    if (!JavaCuda.columnMin(rows, columns, devPtr, newMatrix.devPtr)) {
      newMatrix.free();
      throw new RuntimeException("Failed to get columnMins");
    }
    return newMatrix;
  }

  @Override
  public Matrix rowMins() {
    final MatrixCudaImpl newMatrix = new MatrixCudaImpl(rows, 1);
    if (!JavaCuda.rowMin(rows, columns, devPtr, newMatrix.devPtr)) {
      newMatrix.free();
      throw new RuntimeException("Failed to get rowMins");
    }
    return newMatrix;
  }

  @Override
  public Matrix columnSums() {
    if (multiplierPtr.isNull()) {
      initMultiplier();
    }
    final MatrixCudaImpl newMatrix = new MatrixCudaImpl(1, columns);
    if (!JavaCuda.gemv('T', rows, columns, 1.0f,
        devPtr, rows, multiplierPtr, 1, 0.0f, newMatrix.getDevicePointer(), 1)) {
      newMatrix.free();
      throw new RuntimeException("Failed to perform matrix-vector multiplication");
    }
    return newMatrix;
  }

  @Override
  public Matrix rowSums() {
    final MatrixCudaImpl newMatrix = new MatrixCudaImpl(rows, 1);
    return rowSums(newMatrix);
  }

  @Override
  public Matrix rowSums(final Matrix result) {
    if (multiplierPtr.isNull()) {
      initMultiplier();
    }

    if (rows != result.getRows() || result.getColumns() != 1) {
      throw new IllegalArgumentException("The size of result matrix is wrong");
    }

    final MatrixCudaImpl resultMatrix = toCudaImpl(result);
    if (!JavaCuda.gemv('N', rows, columns, 1.0f,
        devPtr, rows, multiplierPtr, 1, 0.0f, resultMatrix.getDevicePointer(), 1)) {
      resultMatrix.free();
      throw new RuntimeException("Failed to perform matrix-vector multiplication");
    }
    return resultMatrix;
  }

  @Override
  public float sum() {
    return JavaCuda.sum(length, devPtr);
  }

  @Override
  public boolean compare(final Matrix matrix, final float tolerance) {
    if (rows != matrix.getRows() || columns != matrix.getColumns()) {
      return false;
    }
    final MatrixCudaImpl other = toCudaImpl(matrix);
    return JavaCuda.compare(length, devPtr, other.devPtr, tolerance);
  }

  private void deviceFree() {
    JavaCuda.deviceFree(devPtr);
    devPtr.setNull();
  }

  private void multiplierFree() {
    JavaCuda.deviceFree(multiplierPtr);
    multiplierPtr.setNull();
    multiplierLength = 0;
  }

  public void free() {
    deviceFree();
    multiplierFree();
  }

  public FloatPointer getDevicePointer() {
    return devPtr;
  }

  static MatrixCudaImpl toCudaImpl(final Matrix matrix) {
    if (!(matrix instanceof MatrixCudaImpl)) {
      // TODO #147: different matrix implementations
      throw new IllegalArgumentException("The given matrix should be CUDA backend implementation");
    }
    return (MatrixCudaImpl) matrix;
  }

  void checkElementWiseOpValidity(final Matrix matrix) {
    if (rows != matrix.getRows()) {
      throw new IllegalArgumentException("Cannot perform element-wise operations. The number of rows is different");
    }
    if (columns != matrix.getColumns()) {
      throw new IllegalArgumentException("Cannot perform element-wise operations. The number of columns is different");
    }
  }
}
