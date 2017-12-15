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
package edu.snu.cay.dolphin.async.optimizer.impl.hetero;

import edu.snu.cay.utils.Tuple3;

/**
 * Define optimal configuration computed from optimizer solvers.
 * 1. d : number of data blocks in workers.
 * 2. m : number of model blocks in servers.
 * 3. w : if the machine is worker, w = 1. If the machine is server, w = 0.
 * 4. costSet : computed costSet with this configuration(given d, m, w). This Tuple3 value includes totalCost, compCost,
 *              and commCost.
 */
final class ConfDescriptor {
  private final int[] d;
  private final int[] m;
  private final int[] w;
  private final Tuple3<Double, Double, Double> costSet;

  ConfDescriptor(final int[] d, final int[] m, final int[] w, final Tuple3<Double, Double, Double> costSet) {
    this.d = d;
    this.m = m;
    this.w = w;
    this.costSet = costSet;
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

  Tuple3<Double, Double, Double> getCostSet() {
    return costSet;
  }
}
