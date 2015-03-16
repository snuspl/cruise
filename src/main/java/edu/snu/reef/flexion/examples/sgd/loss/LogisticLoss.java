/**
 * Copyright (C) 2014 Seoul National University
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
package edu.snu.reef.flexion.examples.sgd.loss;

import javax.inject.Inject;

/**
 * Represents the loss function for logistic regression.
 */
public final class LogisticLoss implements Loss {

  @Inject
  public LogisticLoss() {
  }

  @Override
  public final double loss(final double predict, final double label) {
    return Math.log(1 + Math.exp(-predict * label));
  }

  @Override
  public final double gradient(final double predict, final double label) {
    return -label / (1 + Math.exp(-predict * label));
  }
}
