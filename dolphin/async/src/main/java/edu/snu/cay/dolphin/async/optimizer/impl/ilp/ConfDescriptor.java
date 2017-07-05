/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.cay.dolphin.async.optimizer.impl.ilp;

/**
 * Define optimal configuration computed from optimizer solvers.
 */
final class ConfDescriptor {
  private final int[] d;
  private final int[] m;
  private final int[] w;
  private final double cost;

  ConfDescriptor(final int[] d, final int[] m, final int[] w, final double cost) {
    this.d = d;
    this.m = m;
    this.w = w;
    this.cost = cost;
  }

  int[] getD() {
    return d;
  }

  int[] getM() {
    return m;
  }

  int[] getRole() {
    return w;
  }

  double getCost() {
    return cost;
  }
}
