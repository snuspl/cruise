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
package edu.snu.cay.dolphin.async.mlapps.nmf;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Parameters used for non-negative matrix factorization.
 */
final class NMFParameters {

  @NamedParameter(doc = "rank of matrix factorization.", short_name = "rank")
  static final class Rank implements Name<Integer> {
  }

  @NamedParameter(doc = "maximum value for each element", short_name = "max_val", default_value = "1e6")
  static final class MaxValue implements Name<Float> {
  }

  @NamedParameter(doc = "maximum value for initial elements", short_name = "init_max", default_value = "1.0")
  static final class InitialMax implements Name<Float> {
  }

  @NamedParameter(doc = "minimum value for initial elements", short_name = "init_min", default_value = "0.0")
  static final class InitialMin implements Name<Float> {
  }

  @NamedParameter(doc = "whether generated matrices are printed or not at the end", short_name = "print_mat",
                  default_value = "false")
  static final class PrintMatrices implements Name<Boolean> {
  }

  static final class MetricKeys {
    // The key denoting the total loss computed from training data instances.
    static final String TRAINING_LOSS =
        "NMF_TRAINING_LOSS";
  }
}
