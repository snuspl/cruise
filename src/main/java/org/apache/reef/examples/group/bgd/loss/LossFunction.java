/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.examples.group.bgd.loss;

import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * Interface for Loss Functions.
 */
@DefaultImplementation(SquaredErrorLossFunction.class)
public interface LossFunction {

  /**
   * Computes the loss incurred by predicting f, if y is the true label.
   *
   * @param y the label
   * @param f the prediction
   * @return the loss incurred by predicting f, if y is the true label.
   */
  double computeLoss(final double y, final double f);

  /**
   * Computes the gradient with respect to f, if y is the true label.
   *
   * @param y the label
   * @param f the prediction
   * @return the gradient with respect to f
   */
  double computeGradient(final double y, final double f);
}
