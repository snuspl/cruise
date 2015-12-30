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

import edu.snu.cay.common.math.vector.breeze.Vector;

public final class Row {
  private final double output;
  private final Vector feature;

  public Row(final double output, final Vector feature) {
    this.output = output;
    this.feature = feature;
  }

  public double getOutput() {
    return output;
  }

  public Vector getFeature() {
    return feature;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final Row row = (Row) o;

    if (Double.compare(row.output, output) != 0) {
      return false;
    }
    return feature.equals(row.feature);

  }

  @Override
  public int hashCode() {
    int result;
    long temp;
    temp = Double.doubleToLongBits(output);
    result = (int) (temp ^ (temp >>> 32));
    result = 31 * result + feature.hashCode();
    return result;
  }
}
