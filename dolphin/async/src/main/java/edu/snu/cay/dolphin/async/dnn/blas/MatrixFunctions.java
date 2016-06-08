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

/**
 * Utility class that provides functions for {@link Matrix}.
 */
public final class MatrixFunctions {

  private MatrixFunctions() {
  }

  public static Matrix neg(final Matrix m) {
    return negi(m.dup());
  }

  public static Matrix negi(final Matrix m) {
    for (int i = 0; i < m.getLength(); ++i) {
      m.put(i, -m.get(i));
    }
    return m;
  }

  public static Matrix exp(final Matrix m) {
    return expi(m.dup());
  }

  public static Matrix expi(final Matrix m) {
    for (int i = 0; i < m.getLength(); ++i) {
      m.put(i, (float) Math.exp(m.get(i)));
    }
    return m;
  }

  public static Matrix pow(final Matrix m, final float c) {
    return powi(m.dup(), c);
  }

  public static Matrix powi(final Matrix m, final float c) {
    for (int i = 0; i < m.getLength(); ++i) {
      m.put(i, (float) Math.pow(m.get(i), c));
    }
    return m;
  }

  public static Matrix abs(final Matrix m) {
    return absi(m.dup());
  }

  public static Matrix absi(final Matrix m) {
    for (int i = 0; i < m.getLength(); ++i) {
      m.put(i, Math.abs(m.get(i)));
    }
    return m;
  }

  public static Matrix signum(final Matrix m) {
    return signumi(m.dup());
  }

  public static Matrix signumi(final Matrix m) {
    for (int i = 0; i < m.getLength(); ++i) {
      m.put(i, Math.signum(m.get(i)));
    }
    return m;
  }

  public static Matrix tanh(final Matrix m) {
    return tanhi(m.dup());
  }

  public static Matrix tanhi(final Matrix m) {
    for (int i = 0; i < m.getLength(); ++i) {
      m.put(i, (float) Math.tanh(m.get(i)));
    }
    return m;
  }
}
