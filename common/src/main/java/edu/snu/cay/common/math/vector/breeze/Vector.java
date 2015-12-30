/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.cay.common.math.vector.breeze;

/**
 * Interface for vector whose elements are {@code double} values.
 */
public interface Vector extends Iterable<VectorEntry> {

  /**
   * Returns the number of elements in vector.
   * @return number of elements
   */
  int length();

  /**
   * Returns the number of active elements in vector.
   * @return number of active elements
   */
  int activeSize();

  /**
   * Returns true if the vector is dense, false if sparse.
   * @return true if the vector is dense, false if sparse
   */
  boolean isDense();

  /**
   * Gets an element specified by given index.
   * @param index an index in range [0, length)
   * @return element specified by given index
   */
  double get(int index);

  /**
   * Sets an element to given value.
   * @param index an index in range [0, length)
   * @param value value to be set
   */
  void set(int index, double value);

  /**
   * Returns a new vector same as this one.
   * @return copied new vector
   */
  Vector copy();

  /**
   * Element-wise vector addition (in place).
   * @param vector operand vector
   * @return operation result
   */
  Vector addi(Vector vector);

  /**
   * Element-wise vector addition.
   * @param vector operand vector
   * @return operation result
   */
  Vector add(Vector vector);

  /**
   * Element-wise vector subtraction (in place).
   * @param vector operand vector
   * @return operation result
   */
  Vector subi(Vector vector);

  /**
   * Element-wise vector subtraction.
   * @param vector operand vector
   * @return operation result
   */
  Vector sub(Vector vector);

  /**
   * Multiplies a scala to all elements (in place).
   * @param value operand scala
   * @return operation result
   */
  Vector scalei(double value);

  /**
   * Multiplies a scala to all elements.
   * @param value operand scala
   * @return operation result
   */
  Vector scale(double value);

  /**
   * In place axpy (y += a * x) operation.
   * @param value operand a
   * @param vector operand x
   * @return operation result
   */
  Vector axpy(double value, Vector vector);

  /**
   * Computes inner product.
   * @param vector operand vector
   * @return operation result
   */
  double dot(Vector vector);
}
