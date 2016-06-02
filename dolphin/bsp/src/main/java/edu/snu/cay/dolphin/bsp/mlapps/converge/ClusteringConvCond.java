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
package edu.snu.cay.dolphin.bsp.mlapps.converge;

import org.apache.mahout.math.Vector;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * Interface for a check function that decides whether
 * a clustering algorithm has converged or not.
 */
@DefaultImplementation(ClusteringConvEuclidean.class)
public interface ClusteringConvCond {

  /**
   * Check convergence conditions.
   */
  boolean checkConvergence(Iterable<Vector> centroids);
}
