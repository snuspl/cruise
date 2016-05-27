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
package edu.snu.cay.dolphin.bsp.examples.ml.data;

import org.apache.mahout.math.Vector;

import javax.inject.Inject;

/**
 * Implements DistanceMeasure (Default).
 */
public final class EuclideanDistance implements VectorDistanceMeasure {

  @Inject
  private EuclideanDistance() {
  }

  @Override
  public double distance(final Vector v1, final Vector v2) {
    if (v1.size() != v2.size()) {
      throw new IllegalArgumentException("Vector dimensions are not consistent");
    }
    return Math.sqrt(v1.getDistanceSquared(v2));
  }
}
