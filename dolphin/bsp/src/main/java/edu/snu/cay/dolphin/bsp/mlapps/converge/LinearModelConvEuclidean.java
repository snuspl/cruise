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
package edu.snu.cay.dolphin.bsp.mlapps.converge;

import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.dolphin.bsp.mlapps.parameters.ConvergenceThreshold;
import edu.snu.cay.dolphin.bsp.mlapps.data.LinearModel;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * Default implementation of LinearModelConvCond.
 * Algorithm converges when its model is changed less than
 * a certain threshold in terms of Euclidean distance after an iteration.
 */
public class LinearModelConvEuclidean implements  LinearModelConvCond {
  private LinearModel oldModel;
  private final double convergenceThreshold;

  @Inject
  public LinearModelConvEuclidean(@Parameter(ConvergenceThreshold.class) final double convergenceThreshold) {
    this.convergenceThreshold = convergenceThreshold;
  }

  @Override
  public final boolean checkConvergence(final LinearModel model) {
    if (oldModel == null) {
      oldModel = new LinearModel(model.getParameters().copy());
      return false;
    } else {
      return distance(oldModel.getParameters(), model.getParameters()) < convergenceThreshold;
    }
  }

  public final double distance(final Vector v1, final Vector v2) {
    // TODO #294: After #294 is resolved, this method will use EuclideanDistance.distance()
    final Vector diff = v1.sub(v2);
    return Math.sqrt(diff.dot(diff));
  }
}
