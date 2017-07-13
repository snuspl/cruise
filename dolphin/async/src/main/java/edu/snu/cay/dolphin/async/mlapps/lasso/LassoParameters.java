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

/**
 * Parameters used for Lasso.
 */
final class LassoParameters {

  static final class MetricKeys {

    // The key denoting the sum of loss computed from the training data instances.
    static final String TRAINING_LOSS =
        "LASSO_TRAINING_LOSS";

    // The key denoting the sum of loss computed from the test data instances.
    static final String TEST_LOSS =
        "LASSO_TEST_LOSS";
  }
}
