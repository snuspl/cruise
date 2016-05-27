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
package edu.snu.cay.dolphin.bsp.examples.ml.data;

public class LinearRegSummary {
  private final LinearModel model;
  private int count = 0;
  private double loss = 0;

  public LinearRegSummary(final LinearModel model, final int count, final double loss) {
    this.model = model;
    this.count = count;
    this.loss = loss;
  }

  public void plus(final LinearRegSummary summary) {
    this.model.setParameters(this.model.getParameters().add(summary.getModel().getParameters()));
    this.count += summary.count;
    this.loss += summary.loss;
  }

  public LinearModel getModel() {
    return model;
  }

  public int getCount() {
    return count;
  }

  public double getLoss() {
    return loss;
  }
}
