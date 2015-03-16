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
package edu.snu.reef.flexion.examples.sgd.data;

import org.apache.mahout.math.Vector;

public final class Example {
  private final double label;
  private final Vector point;

  public Example(final double label, final Vector point) {
    this.label = label;
    this.point = point;
  }

  public final double getLabel() {
    return label;
  }

  public final Vector getPoint() {
    return point;
  }
}
