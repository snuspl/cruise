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
import edu.snu.cay.dolphin.async.ModelHolder;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Provides a reference to model data used in MLR app.
 * This implementation allows multiple threads to update their model locally without any side-effect from others.
 */
@ThreadSafe
final class MLRModelHolder implements ModelHolder<MLRModel> {
  private final ThreadLocal<MLRModel> model;

  MLRModelHolder(final Vector[] params) {
    this.model = ThreadLocal.withInitial(() -> new MLRModel(params.clone()));
  }

  /**
   * Gets the up-to-date model locally assigned to this thread.
   */
  @Override
  public MLRModel getModel() {
    return model.get();
  }
}
