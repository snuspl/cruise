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
package edu.snu.cay.dolphin.async.mlapps.gbt;

import edu.snu.cay.common.math.linalg.Vector;

/**
 * Abstraction for training data used in GBRT, consisting of identity value, feature vector and y-value.
 */
final class GBTData {
  private final int identity;
  private final Vector feature;
  private final double value;

  GBTData(final int identity, final Vector feature, final double value) {
    this.identity = identity;
    this.feature = feature;
    this.value = value;
  }

  /**
   * @return Identity value of the training data instance.
   */
  int getIdentity() {
    return identity;
  }

  /**
   * @return Feature vector of the training data instance.
   */
  Vector getFeature() {
    return feature;
  }

  /**
   * @return Y-value of the training data instance.
   */
  double getValue() {
    return value;
  }
}
