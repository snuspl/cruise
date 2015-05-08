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
package edu.snu.reef.dolphin.examples.ml.algorithms.pagerank;

import edu.snu.reef.dolphin.core.DataParser;
import edu.snu.reef.dolphin.core.ParseException;
import edu.snu.reef.dolphin.core.UserComputeTask;
import edu.snu.reef.dolphin.examples.ml.data.PageRankSummary;
import edu.snu.reef.dolphin.groupcomm.interfaces.DataBroadcastReceiver;
import edu.snu.reef.dolphin.groupcomm.interfaces.DataReduceSender;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PageRankCmpTask extends UserComputeTask
    implements DataReduceSender<PageRankSummary>, DataBroadcastReceiver<Map<Integer,Double>> {

  /**
   * Adjacency list data parser
   */
  private DataParser<Map<Integer,List<Integer>>> dataParser;

  /**
   * Subgraph consists a node and its outgoing neighbor set
   */
  private Map<Integer,List<Integer>> subgraphs;

  /**
   * Map of current rank
   */
  private Map<Integer,Double> rank;

  /**
   * Map of contributed increment to outgoing neighbor nodes
   */
  private Map<Integer,Double> increment;

  /**
   * This class is instantiated by TANG
   * Constructs a single Compute Task for PageRank
   * @param dataParser
   */
  @Inject
  public PageRankCmpTask(DataParser<Map<Integer,List<Integer>>> dataParser) {
    this.dataParser = dataParser;
  }

  /**
   * Load a split subgraph and set the initial rank for each node
   * @throws ParseException
   */
  @Override
  public void initialize() throws ParseException {
    subgraphs = dataParser.get();
    rank = new HashMap<>();
    for (Integer key : subgraphs.keySet()) {
      rank.put(key, 1.0d);
    }
  }

  @Override
  public final void run(int iteration) {
    increment = new HashMap<>();
    for (Map.Entry<Integer,List<Integer>> entry : subgraphs.entrySet()) {
      final Integer nodeId = entry.getKey();
      final List<Integer> outList = entry.getValue();

      final double contribution = rank.get(nodeId) * 1.0d / outList.size();
      for (Integer adjNode : outList) {
        if (increment.containsKey(adjNode)) {
          increment.put(adjNode, increment.get(adjNode) + contribution);
        } else {
          increment.put(adjNode, contribution);
        }
      }
    }
  }

  /**
   * Receive the updated rank of all nodes from previous run.
   * At the iteration 0, the rank is empty.
   * @param iteration
   * @param rank
   */
  @Override
  public final void receiveBroadcastData(int iteration, Map<Integer,Double> rank) {
    if (iteration > 1) {
      this.rank = rank;
    }
  }

  /**
   * Send the new increment for outgoing link nodes in this subgraph
   * @param iteration
   * @return
   */
  @Override
  public PageRankSummary sendReduceData(int iteration) {
    return new PageRankSummary(this.increment);
  }
}
