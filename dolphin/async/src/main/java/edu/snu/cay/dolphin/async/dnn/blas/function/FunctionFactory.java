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
package edu.snu.cay.dolphin.async.dnn.blas.function;

/**
 * Factory class for {@link Function}.
 */
public final class FunctionFactory {

  private FunctionFactory() {
  }

  // Function singletons
  private static final Function SIGMOID = new Sigmoid();
  private static final Function IDENTITY = new Identity();
  private static final Function RELU = new ReLU();
  private static final Function TANH = new Tanh();
  private static final Function POWER = new Power(); // the square function
  private static final Function ABSOLUTE = new Absolute();
  private static final Function SOFTMAX = new Softmax();

  public static Function getSingleInstance(final String name) {
    switch (name.toLowerCase()) {
    case "identity":
      return IDENTITY;
    case "sigmoid":
      return SIGMOID;
    case "relu":
      return RELU;
    case "tanh":
      return TANH;
    case "pow":
      return POWER;
    case "abs":
      return ABSOLUTE;
    case "softmax":
      return SOFTMAX;
    default:
      throw new IllegalArgumentException("Unsupported function: " + name);
    }
  }
}
