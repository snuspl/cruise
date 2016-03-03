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
package edu.snu.cay.async.examples.recommendation;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Parameters used for non-negative matrix factorization.
 */
final class NMFParameters {

  @NamedParameter(doc = "number of rows in input matrix.", short_name = "row")
  static final class NumRows implements Name<Integer> {
  }

  @NamedParameter(doc = "number of columns in input matrix.", short_name = "col")
  static final class NumColumns implements Name<Integer> {
  }

  @NamedParameter(doc = "rank of matrix factorization.", short_name = "rank")
  static final class Rank implements Name<Integer> {
  }

  @NamedParameter(doc = "step size for stochastic gradient descent", short_name = "stepSize")
  static final class StepSize implements Name<Double> {
  }

  @NamedParameter(doc = "regularization constant value", short_name = "lambda")
  static final class Lambda implements Name<Double> {
  }

  @NamedParameter(doc = "maximum value for each element", short_name = "maxVal", default_value = "1e6")
  static final class MaxValue implements Name<Double> {
  }

  @NamedParameter(doc = "maximum value for initial elements", short_name = "initMax", default_value = "1.0")
  static final class InitialMax implements Name<Double> {
  }

  @NamedParameter(doc = "minimum value for initial elements", short_name = "initMin", default_value = "0.0")
  static final class InitialMin implements Name<Double> {
  }
}
