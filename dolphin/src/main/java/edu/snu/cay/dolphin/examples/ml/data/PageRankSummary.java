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
package edu.snu.cay.dolphin.examples.ml.data;

import java.util.Map;

public class PageRankSummary {
  private final Map<Integer, Double> contributionSum;

  public PageRankSummary(final Map<Integer, Double> contributionSum) {
    this.contributionSum = contributionSum;
  }

  public void plus(final PageRankSummary summary) {
    for (final Map.Entry<Integer, Double> entry : summary.getModel().entrySet()) {
      final Integer nodeId = entry.getKey();
      final Double contribution = entry.getValue();

      if (contributionSum.containsKey(nodeId)) {
        contributionSum.put(nodeId, contributionSum.get(nodeId) + contribution);
      } else {
        contributionSum.put(nodeId, contribution);
      }
    }
  }

  public Map<Integer, Double> getModel() {
    return contributionSum;
  }
}
