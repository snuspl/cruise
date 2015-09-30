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

public final class LinearModel implements Model {
  private Vector parameters;

  public LinearModel(final Vector parameters) {
    this.parameters = parameters;
  }

  public Vector getParameters() {
    return parameters;
  }

  @Override
  public void setParameters(final Vector parameters) {
    this.parameters = parameters;
  }

  @Override
  public double predict(final Vector point) {
    return parameters.dot(point);
  }

  @SuppressWarnings("boxing")
  @Override
  public String toString() {
    final StringBuffer buffer = new StringBuffer();
    boolean addPlus = false;
    for (final Vector.Element element : parameters.nonZeroes()) {
      if (addPlus) {
        buffer.append(" + ");
      }
      buffer.append(String.format("%e", element.get()));
      final int index = element.index();
      if (index != parameters.size() - 1) {  // not constant term
        buffer.append(String.format(" * x%d", index + 1));
      }
      addPlus = true;
    }
    return buffer.toString();
  }
}
