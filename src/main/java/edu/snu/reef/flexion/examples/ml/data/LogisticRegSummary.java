/**
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
package edu.snu.reef.flexion.examples.ml.data;

public class LogisticRegSummary {

  private final LinearModel model;
  private int count;
  private int posNum;
  private int negNum;

  public LogisticRegSummary(LinearModel model, int count, int posNum, int negNum) {
    this.model = model;
    this.count = count;
    this.posNum = posNum;
    this.negNum = negNum;
  }

  public void plus(LogisticRegSummary summary) {
    this.model.setParameters(this.model.getParameters().plus(summary.getModel().getParameters()));
    this.count += summary.count;
    this.posNum += summary.posNum;
    this.negNum += summary.negNum;
  }

  public LinearModel getModel() {
    return model;
  }

  public int getCount() {
    return count;
  }

  public int getPosNum() {
    return posNum;
  }

  public int getNegNum() {
    return negNum;
  }

}
