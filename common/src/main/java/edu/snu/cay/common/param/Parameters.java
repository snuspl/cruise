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

  @NamedParameter(doc = "Desired memory size for each evaluator (MBs)",
                  short_name = "evalSize",
                  default_value = "128")
  public final class EvaluatorSize implements Name<Integer> {
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
                  short_name = "maxNumEvalLocal",
                  default_value = "2")
  public final class LocalRuntimeMaxNumEvaluators implements Name<Integer> {
  }

  @NamedParameter(doc = "Maximum number of iterations to run before termination",
                  short_name = "maxIter")
  public final class Iterations implements Name<Integer> {
  }
}
