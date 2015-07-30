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
package edu.snu.cay.dolphin.examples.ml.converge;

import edu.snu.cay.dolphin.examples.ml.parameters.ConvergenceThreshold;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Map;

/**
 * Default implementation of PageRankConvCond
 * Algorithm converges when every ranks has moved less than
 * a certain threshold after an iteration
 */
public final class PageRankConvEuclidean implements PageRankConvCond {
  private Map<Integer, Double> oldRank;
  private final double convergenceThreshold;

  @Inject
  public PageRankConvEuclidean(
          @Parameter(ConvergenceThreshold.class) final double convergenceThreshold) {
    this.convergenceThreshold = convergenceThreshold;
  }

  @Override
  public final boolean checkConvergence(final Map<Integer, Double> rank) {
    if (rank.size() == 0) {
      return false;
    }

    if (oldRank == null) {
      oldRank = rank;
      return false;
    }

    boolean hasConverged = true;
    for (final Integer key : oldRank.keySet()) {
      if (Math.abs(oldRank.get(key) - rank.get(key)) > convergenceThreshold) {
        hasConverged = false;
        break;
      }
    }
    oldRank = rank;

    return hasConverged;
  }
}
