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
package edu.snu.cay.dolphin.examples.ml.regularization;

import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.dolphin.examples.ml.parameters.Lambda;
import edu.snu.cay.dolphin.examples.ml.data.Model;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * Class that represents ||w||_2^2.
 */
public final class L2Regularization implements Regularization {
  private double lambda;

  @Inject
  public L2Regularization(@Parameter(Lambda.class) final double lambda) {
    this.lambda = lambda;
  }

  @Override
  public double regularize(final Model model) {
    return lambda * model.getParameters().dot(model.getParameters()) / 2;
  }

  @Override
  public Vector gradient(final Model model) {
    return model.getParameters().scale(lambda);
  }
}
