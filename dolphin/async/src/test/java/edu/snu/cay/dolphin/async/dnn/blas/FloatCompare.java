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
 * Utility class for comparing floats with tolerance.
 */
final class FloatCompare {
  static final float TOLERANCE = 1.0E-06F;

  private FloatCompare() {
  }

  public static void assertEquals(final float expected, final float actual) {
    org.junit.Assert.assertEquals(expected, actual, TOLERANCE);
  }

  public static void assertArrayEquals(final float[] expecteds, final float[] actuals) {
    org.junit.Assert.assertArrayEquals(expecteds, actuals, TOLERANCE);
  }
}
