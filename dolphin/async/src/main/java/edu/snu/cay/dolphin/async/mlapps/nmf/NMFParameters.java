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

  @NamedParameter(doc = "step size for stochastic gradient descent", short_name = "step_size")
  static final class StepSize implements Name<Double> {
  }

  @NamedParameter(doc = "regularization constant value", short_name = "lambda", default_value = "0.0")
  static final class Lambda implements Name<Double> {
  }

  @NamedParameter(doc = "maximum value for each element", short_name = "max_val", default_value = "1e6")
  static final class MaxValue implements Name<Double> {
  }

  @NamedParameter(doc = "maximum value for initial elements", short_name = "init_max", default_value = "1.0")
  static final class InitialMax implements Name<Double> {
  }

  @NamedParameter(doc = "minimum value for initial elements", short_name = "init_min", default_value = "0.0")
  static final class InitialMin implements Name<Double> {
  }

  @NamedParameter(doc = "whether generated matrices are printed or not at the end", short_name = "print_mat",
                  default_value = "false")
  static final class PrintMatrices implements Name<Boolean> {
  }

  @NamedParameter(doc = "minimum number of input rows when logging execution status", short_name = "log_period",
                  default_value = "0")
  static final class LogPeriod implements Name<Integer> {
  }

  static final class MetricKeys {
    // The key denoting avg loss for an iteration.
    static final String AVG_LOSS =
        "NMF_WORKER_AVG_LOSS";

    // The key denoting loss sum for an iteration.
    static final String SUM_LOSS =
        "NMF_WORKER_SUM_LOSS";

    // The key denoting DvT for an iteration.
    static final String DVT =
        "NMF_WORKER_DVT";

    // The key denoting RvT for an iteration.
    static final String RVT =
        "NMF_WORKER_RVT";
  }
}
