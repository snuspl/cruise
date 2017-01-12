/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.cay.dolphin.async.mlapps.nmf;

import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.utils.Copyable;

import java.util.HashMap;
import java.util.Map;

/**
 * Encapsulates the model data in NMF app.
 */
final class NMFModel implements Copyable<NMFModel> {
  private final Map<Integer, Vector> rMatrix;
  private final Map<Integer, Vector> rGradient;

  NMFModel(final Map<Integer, Vector> rMatrix) {
    this.rMatrix = rMatrix;
    this.rGradient = new HashMap<>();
  }

  private NMFModel(final Map<Integer, Vector> rMatrix, final Map<Integer, Vector> rGradient) {
    this.rMatrix = rMatrix;
    this.rGradient = rGradient;
  }

  @Override
  public NMFModel copyOf() {
    return new NMFModel(copyMap(rMatrix), copyMap(rGradient));
  }

  /**
   * @return the R matrix
   */
  Map<Integer, Vector> getRMatrix() {
    return rMatrix;
  }

  /**
   * @return the gradient of R matrix
   */
  Map<Integer, Vector> getRGradient() {
    return rGradient;
  }

  /**
   * Creates a deep copy of a map. The types of entries are restricted to those we use in NMF app.
   */
  private Map<Integer, Vector> copyMap(final Map<Integer, Vector> toCopy) {
    final Map<Integer, Vector> copied = new HashMap<>(toCopy.size());
    toCopy.forEach((integer, vectorEntries) -> copied.put(integer, vectorEntries.copy()));
    return copied;
  }
}