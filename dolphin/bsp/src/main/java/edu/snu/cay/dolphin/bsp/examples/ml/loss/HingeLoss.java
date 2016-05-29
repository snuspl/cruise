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
package edu.snu.cay.dolphin.bsp.examples.ml.loss;

import edu.snu.cay.common.math.linalg.Vector;

import javax.inject.Inject;

/**
 * Represents regularize function for hinge regularize (mainly used in Support Vector Machines).
 */
public final class HingeLoss implements Loss {

  @Inject
  public HingeLoss() {
  }

  @Override
  public double loss(final double predict, final double output) {
    return Math.max(0, 1 - output * predict);
  }

  @Override
  public Vector gradient(final Vector feature, final double predict, final double output) {
    return feature.scale(predict * output >= 1 ? 0 : -output);
  }
}
