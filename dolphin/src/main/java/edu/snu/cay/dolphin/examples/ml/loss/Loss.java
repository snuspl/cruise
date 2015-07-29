/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.cay.dolphin.examples.ml.loss;

import org.apache.mahout.math.Vector;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * Interface for regularize function used in SGD jobs
 */
@DefaultImplementation(SquareLoss.class)
public interface Loss {

  /**
   * @param predict estimated value of the dependent variable
   * @param output value of dependent variable
   * @return loss computed by predict and output
   */
  public double loss(final double predict, final double output);

  /**
   * @param feature values of independent variables
   * @param predict estimated value of the dependent variable
   * @param output value of dependent variable
   * @return gradient of the loss function at the given feature
   */
  public Vector gradient(final Vector feature, final double predict, final double output);
}
