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

  @NamedParameter(doc = "initial value of the step size", short_name = "init_step_size")
  static final class InitialStepSize implements Name<Float> {
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
