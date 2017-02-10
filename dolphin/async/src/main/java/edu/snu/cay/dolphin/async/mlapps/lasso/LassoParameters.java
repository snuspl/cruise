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
package edu.snu.cay.dolphin.async.mlapps.lasso;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Parameters used for Lasso.
 */
final class LassoParameters {

  @NamedParameter(doc = "input dimension", short_name = "features")
  static final class NumFeatures implements Name<Integer> {
  }

  @NamedParameter(doc = "value of the step size", short_name = "step_size")
  static final class StepSize implements Name<Double> {
  }

  @NamedParameter(doc = "regularization constant", short_name = "lambda")
  static final class Lambda implements Name<Double> {
  }

  @NamedParameter(doc = "standard deviation of the gaussian distribution used for initializing model parameters",
      short_name = "model_gaussian",
      default_value = "0.001")
  static final class ModelGaussian implements Name<Double> {
  }

  @NamedParameter(doc = "ratio which step size decreases by (multiplicative). this value must be larger than 0 " +
          "and less than or equal to 1. if decay_rate=1.0, decaying process is turned off.",
          short_name = "decay_rate",
          default_value = "0.95")
  static final class DecayRate implements Name<Double> {
  }

  @NamedParameter(doc = "number of iterations to wait until learning rate decreases (periodic). this value must be " +
          "a positive value.",
          short_name = "decay_period",
          default_value = "5")
  static final class DecayPeriod implements Name<Integer> {
  }
}
