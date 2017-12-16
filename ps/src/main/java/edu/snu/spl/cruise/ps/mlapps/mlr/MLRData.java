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
package edu.snu.spl.cruise.ps.mlapps.mlr;

import edu.snu.spl.cruise.common.math.linalg.Vector;

/**
 * Abstraction for training data used in MLR, consisting of feature vector and label (class).
 */
final class MLRData {
  private final Vector feature;
  private final int label;

  MLRData(final Vector feature, final int label) {
    this.feature = feature;
    this.label = label;
  }

  /**
   * @return Feature vector of the training data instance.
   */
  Vector getFeature() {
    return feature;
  }

  /**
   * @return Label of the training data instance.
   */
  int getLabel() {
    return label;
  }
}
