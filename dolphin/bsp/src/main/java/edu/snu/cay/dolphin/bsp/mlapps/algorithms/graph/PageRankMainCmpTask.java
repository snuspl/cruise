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
package edu.snu.cay.dolphin.bsp.mlapps.algorithms.graph;

import edu.snu.cay.dolphin.bsp.core.ParseException;
import edu.snu.cay.dolphin.bsp.core.UserComputeTask;
import edu.snu.cay.dolphin.bsp.mlapps.data.PageRankSummary;
import edu.snu.cay.dolphin.bsp.groupcomm.interfaces.DataReduceSender;
import edu.snu.cay.dolphin.bsp.mlapps.data.AdjacencyListDataType;
import edu.snu.cay.dolphin.bsp.groupcomm.interfaces.DataBroadcastReceiver;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * PageRank algorithm compute class.
 *
 * Reference
 * - http://en.wikipedia.org/wiki/PageRank
 * - https://github.com/apache/spark/blob/master/graphx/src/main/scala/org/apache/spark/graphx/lib/PageRank.scala
 */
public class PageRankMainCmpTask extends UserComputeTask
    implements DataReduceSender<PageRankSummary>, DataBroadcastReceiver<Map<Integer, Double>> {

  /**
   * Key used in Elastic Memory to put/get the data.
   */
  private final String dataType;

  /**
   * Memory storage to put/get the data.
   */
  private final MemoryStore memoryStore;

  /**
   * Map of current rank.
   */
  private Map<Integer, Double> rank;

  /**
   * Map of contributed increment to outgoing neighbor nodes.
   */
  private final Map<Integer, Double> increment = new HashMap<>();

  /**
   * Constructs a single Compute Task for PageRank.
   * This class is instantiated by TANG.
   * @param dataType
   * @param memoryStore
   */
  @Inject
  public PageRankMainCmpTask(@Parameter(AdjacencyListDataType.class) final String dataType,
                             final MemoryStore memoryStore) {
    this.dataType = dataType;
    this.memoryStore = memoryStore;
  }

  /**
   * Load a split subgraph and set the initial rank for each node.
   * @throws ParseException
   */
  @Override
  public void initialize() throws ParseException {
    final Map<Long, List<Integer>> subgraphs = memoryStore.getAll(dataType);
    // Map of current rank
    rank = new HashMap<>();
    for (final Long key : subgraphs.keySet()) {
      rank.put(key.intValue(), 1.0d);
    }
  }

  @Override
  public final void run(final int iteration) {
    increment.clear();
    final Map<Long, List<Integer>> subgraphs = memoryStore.getAll(dataType);
    for (final Map.Entry<Long, List<Integer>> entry : subgraphs.entrySet()) {
      final Integer nodeId = entry.getKey().intValue();
      final List<Integer> outList = entry.getValue();

      // Add itself to ensure existence in graph
      if (!increment.containsKey(nodeId)) {
        increment.put(nodeId, 0.0d);
      }
      // Skip distribution if it has no outbound neighbor
      if (outList.size() == 0) {
        continue;
      }

      // The rank of node is distributed evenly to its outbound neighbor nodes.
      // Compute a contribution per each node
      final double contribution = rank.get(nodeId) * 1.0d / outList.size();
      // Add the contribution to each node
      for (final Integer adjNode : outList) {
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
   * @param rankData
   */
  @Override
  public final void receiveBroadcastData(final int iteration, final Map<Integer, Double> rankData) {
    if (iteration < 1) {
      return;
    }
    rank = rankData;
  }

  /**
   * Send the new increment for outgoing link nodes in this subgraph.
   * @param iteration
   * @return
   */
  @Override
  public PageRankSummary sendReduceData(final int iteration) {
    return new PageRankSummary(this.increment);
  }
}
