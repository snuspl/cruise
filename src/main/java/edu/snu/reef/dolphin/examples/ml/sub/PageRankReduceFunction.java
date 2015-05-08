/**
 * Copyright (C) 2015 SK Telecom
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
package edu.snu.reef.dolphin.examples.ml.sub;

import com.microsoft.reef.io.network.group.operators.Reduce;
import edu.snu.reef.dolphin.examples.ml.data.PageRankSummary;

import javax.inject.Inject;

public class PageRankReduceFunction implements Reduce.ReduceFunction<PageRankSummary> {

  @Inject
  public PageRankReduceFunction() {
  }

  @Override
  public final PageRankSummary apply(Iterable<PageRankSummary> summaryList) {
    PageRankSummary reducedSummary = null;
    for (final PageRankSummary summary : summaryList) {
      if (reducedSummary==null) {
        reducedSummary = summary;
      } else {
        reducedSummary.plus(summary);
      }
    }
    return reducedSummary;
  }
}
