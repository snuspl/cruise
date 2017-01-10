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
package edu.snu.cay.dolphin.async.mlapps.mlr;

import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.utils.Copyable;

/**
 * Encapsulates the model data in MLR app.
 */
final class MLRModel implements Copyable<MLRModel> {
  private final Vector[] params;

  MLRModel(final Vector[] params) {
    this.params = params;
  }

  /**
   * @return a vector that consists of model parameters.
   */
  Vector[] getParams() {
    return params;
  }

  @Override
  public MLRModel copyOf() {
    final Vector[] copied = new Vector[params.length];
    for (int idx = 0; idx < params.length; idx++) {
      copied[idx] = params[idx].copy();
    }
    return new MLRModel(copied);
  }
}
