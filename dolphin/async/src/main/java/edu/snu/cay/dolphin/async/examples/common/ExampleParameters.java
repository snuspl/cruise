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
package edu.snu.cay.dolphin.async.examples.common;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Common parameter classes for examples.
 */
public final class ExampleParameters {
  @NamedParameter(doc = "All workers will add this integer to the sum", short_name = "delta")
  public final class DeltaValue implements Name<Integer> {
  }

  @NamedParameter(doc = "The number of keys", short_name = "num_keys")
  public final class NumKeys implements Name<Integer> {
  }

  @NamedParameter(doc = "The time to sleep (per training data instance) to simulate the computation in workers",
      short_name = "compute_time_ms", default_value = "300")
  public final class ComputeTimeMs implements Name<Long> {
  }

  @NamedParameter(doc = "The number of data instances to assign each worker",
      short_name = "num_training_data")
  public final class NumTrainingData implements Name<Integer> {
  }

  @NamedParameter(doc = "The number of test data",
      short_name = "num_test_data")
  public final class NumTestData implements Name<Integer> {
  }
}
