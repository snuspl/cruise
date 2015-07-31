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
package edu.snu.cay.dolphin.examples.ml.data;

import org.apache.mahout.math.Vector;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.io.Serializable;

/**
 * Interface for representations of models used in ML job
 */
@DefaultImplementation(LinearModel.class)
public interface Model extends Serializable {

  /**
   * @return a Vector containing values that represent the model
   */
  public Vector getParameters();

  /**
   * @param parameters set this model using these values
   */
  public void setParameters(Vector parameters);

  /**
   * @param point the Vector to perform prediction on
   * @return a predicted class value using the model
   */
  public double predict(Vector point);
}
