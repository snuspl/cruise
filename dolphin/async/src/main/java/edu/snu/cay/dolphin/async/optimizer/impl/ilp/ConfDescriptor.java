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
 * 1. d : number of data blocks in workers.
 * 2. m : number of model blocks in servers.
 * 3. w : if the machine is worker, w = 1. If the machine is server, w = 0.
 * 4. cost : computed cost with this configuration(given d, m, w).
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
