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

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Parameters used for multi-class logistic regression.
 */
final class MLRParameters {

  @NamedParameter(doc = "number of classes", short_name = "classes")
  static final class NumClasses implements Name<Integer> {
  }

  @NamedParameter(doc = "input dimension", short_name = "features")
  static final class NumFeatures implements Name<Integer> {
  }

  @NamedParameter(doc = "initial value of the step size", short_name = "init_step_size")
  static final class InitialStepSize implements Name<Float> {
  }

  @NamedParameter(doc = "regularization constant", short_name = "lambda")
  static final class Lambda implements Name<Float> {
  }

  @NamedParameter(doc = "number of features for each model partition",
      short_name = "features_per_partition")
  static final class NumFeaturesPerPartition implements Name<Integer> {
  }

  @NamedParameter(doc = "standard deviation of the gaussian distribution used for initializing model parameters",
      short_name = "model_gaussian",
      default_value = "0.001")
  static final class ModelGaussian implements Name<Float> {
  }

  @NamedParameter(doc = "ratio which learning rate decreases by (multiplicative). this value must be larger than 0 " +
      "and less than or equal to 1. if decay_rate=1.0, decaying process is turned off.",
      short_name = "decay_rate",
      default_value = "0.9")
  static final class DecayRate implements Name<Float> {
  }

  @NamedParameter(doc = "number of epochs to wait until learning rate decreases (periodic). this value must be " +
      "a positive value.",
      short_name = "decay_period",
      default_value = "5")
  static final class DecayPeriod implements Name<Integer> {
  }

  static final class MetricKeys {

    // The key denoting the sum of loss computed from the training data instances.
    static final String TRAINING_LOSS =
        "MLR_TRAINING_LOSS";

    // The key denoting the sum of loss computed from the test data instances.
    static final String TEST_LOSS =
        "MLR_TEST_LOSS";

    // The key denoting the average of L2-regularized loss computed from the sample of training data instances.
    static final String TRAINING_REG_LOSS_AVG =
        "MLR_TRAINING_REG_LOSS_AVG";

    // The key denoting the average of L2-regularized loss computed from the test data instances.
    static final String TEST_REG_LOSS_AVG =
        "MLR_TEST_REG_LOSS_AVG";

    // The key denoting accuracy (the number of correct inferences by the model / total number of training data inst).
    static final String TRAINING_ACCURACY =
        "MLR_TRAINING_ACCURACY";

    // The key denoting accuracy (the number of correct inferences by the model / total number of test data inst).
    static final String TEST_ACCURACY =
        "MLR_TEST_ACCURACY";
  }
}
