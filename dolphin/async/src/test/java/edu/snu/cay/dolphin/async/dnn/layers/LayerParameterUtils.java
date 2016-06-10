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
package edu.snu.cay.dolphin.async.dnn.layers;

/**
 * Utility class for testing with {@link LayerParameter}.
 */
public final class LayerParameterUtils {

  /**
   * Should not be instantiated.
   */
  private LayerParameterUtils() {
  }

  /**
   * Returns {@code true} if each element of the weight and bias of a layer parameter is equal to another
   * within the specified tolerance.
   *
   * @param a one layer parameter to be tested for equality
   * @param b another layer parameter to be tested for equality
   * @param tolerance the maximum difference for which both numbers are still considered equal
   * @return {@code true} if two layer parameters are considered equal
   */
  public static boolean compare(final LayerParameter a, final LayerParameter b, final float tolerance) {
    return a.getWeightParam().compare(b.getWeightParam(), tolerance)
        && a.getBiasParam().compare(b.getBiasParam(), tolerance);
  }

  /**
   * Returns {@code true} if each element of the weight and bias of a layer parameter is equal to another
   * within the specified tolerance.
   *
   * @param a one layer parameter array to be tested for equality
   * @param b another layer parameter array to be tested for equality
   * @param tolerance the maximum difference for which both numbers are still considered equal
   * @return {@code true} if two layer parameter arrays are considered equal
   */
  public static boolean compare(final LayerParameter[] a, final LayerParameter[] b, final float tolerance) {
    if (a.length != b.length) {
      return false;
    }
    for (int i = 0; i < a.length; ++i) {
      if (!compare(a[i], b[i], tolerance)) {
        return false;
      }
    }
    return true;
  }
}
