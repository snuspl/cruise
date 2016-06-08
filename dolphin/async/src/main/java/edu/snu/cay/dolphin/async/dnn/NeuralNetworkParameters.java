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
package edu.snu.cay.dolphin.async.dnn;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Parameters used for deep neural network jobs.
 */
public final class NeuralNetworkParameters {

  /**
   * Should not be instantiated.
   */
  private NeuralNetworkParameters() {
  }

  @NamedParameter(doc = "delimiter that is used in input file", short_name = "delim", default_value = ",")
  public static class Delimiter implements Name<String> {
  }

  @NamedParameter(doc = "neural network configuration file path", short_name = "conf")
  public static final class ConfigurationPath implements Name<String> {
  }

  @NamedParameter(doc = "backend BLAS library", short_name = "blas", default_value = "jblas")
  public static class BlasLibrary implements Name<String> {
  }
}
