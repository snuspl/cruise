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
package edu.snu.cay.dolphin.async.mlapps.mlr;

import edu.snu.cay.common.math.linalg.Vector;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.logging.Logger;

/**
 * Created by yunseong on 1/6/17.
 */
final class MLRModelHolder {

  private ThreadLocal<Vector[]> model;
  private Vector[] shared;

  @Inject
  MLRModelHolder(@Parameter(MLRParameters.NumClasses.class) final int numClasses) {
    this.shared = new Vector[numClasses];
  }

  // Get the current model. Per-thread in this case.
  Vector[] getModel() {
    return model.get();
  }

  // When pull
  void initialize(final Vector[] newModel) {
    this.shared = newModel;
    this.model = ThreadLocal.withInitial(() -> shared.clone());
  }
}
