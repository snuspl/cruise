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
package edu.snu.cay.common.param;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Common parameter classes for application frameworks.
 */
public final class Parameters {
  @NamedParameter(doc = "Desired memory size for the driver (MBs)",
                  short_name = "driver_memory",
                  default_value = "256")
  public final class DriverMemory implements Name<Integer> {
  }

  @NamedParameter(doc = "Desired memory size for each evaluator (MBs)",
                  short_name = "eval_size",
                  default_value = "128")
  public final class EvaluatorSize implements Name<Integer> {
  }

  @NamedParameter(doc = "The number of CPU cores for each evaluator",
                  short_name = "num_eval_cores",
                  default_value = "1")
  public final class NumEvaluatorCores implements Name<Integer> {
  }

  @NamedParameter(doc = "File or directory to read input data from",
                  short_name = "input")
  public final class InputDir implements Name<String> {
  }

  @NamedParameter(doc = "Whether or not to run on local runtime",
                  short_name = "local",
                  default_value = "true")
  public final class OnLocal implements Name<Boolean> {
  }

  @NamedParameter(doc = "Desired number of evaluators to run the job with",
                  short_name = "split",
                  default_value = "1")
  public final class Splits implements Name<Integer> {
  }

  @NamedParameter(doc = "Time allowed until job ends",
                  short_name = "timeout",
                  default_value = "100000")
  public final class Timeout implements Name<Integer> {
  }

  @NamedParameter(doc = "Maximum number of local runtime evaluators, must be at least Data Loading Splits + 1",
                  short_name = "max_num_eval_local",
                  default_value = "2")
  public final class LocalRuntimeMaxNumEvaluators implements Name<Integer> {
  }

  @NamedParameter(doc = "Maximum number of iterations to run before termination",
                  short_name = "max_iter")
  public final class Iterations implements Name<Integer> {
  }

  @NamedParameter(doc = "The fraction of the container memory NOT to use for the Java Heap",
                  short_name = "heap_slack",
                  default_value = "0.0")
  public final class JVMHeapSlack implements Name<Double> {
  }

  @NamedParameter(doc = "Number of mini-batches",
      short_name = "num_mini_batch",
      default_value = "1")
  public final class MiniBatches implements Name<Integer> {
  }

  @NamedParameter(doc = "Mini-batch size in number of training data instances",
      short_name = "mini_batch_size")
  public final class MiniBatchSize implements Name<Integer> {
  }

  @NamedParameter(doc = "The minimum cost benefit (in a ratio) for which system optimization occurs. " +
      "0 = optimization occurs for any benefit greater than 0.",
      short_name = "opt_benefit_threshold",
      default_value = "0")
  public final class OptimizationBenefitThreshold implements Name<Double> {
  }
}
