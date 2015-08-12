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
package edu.snu.cay.dolphin.examples.ml.algorithms.graph;

import edu.snu.cay.dolphin.core.OutputStreamProvider;
import edu.snu.cay.dolphin.examples.ml.parameters.MaxIterations;
import edu.snu.cay.dolphin.core.UserControllerTask;
import edu.snu.cay.dolphin.examples.ml.converge.PageRankConvCond;
import edu.snu.cay.dolphin.examples.ml.data.PageRankSummary;
import edu.snu.cay.dolphin.examples.ml.parameters.DampingFactor;
import edu.snu.cay.dolphin.groupcomm.interfaces.DataBroadcastSender;
import edu.snu.cay.dolphin.groupcomm.interfaces.DataReduceReceiver;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * PageRank algorithm control class.
 * 
 * Reference
 * - http://en.wikipedia.org/wiki/PageRank
 * - https://github.com/apache/spark/blob/master/graphx/src/main/scala/org/apache/spark/graphx/lib/PageRank.scala
 */
public class PageRankCtrlTask extends UserControllerTask
    implements DataReduceReceiver<PageRankSummary>, DataBroadcastSender<Map<Integer, Double>> {
  private static final Logger LOG = Logger.getLogger(PageRankCtrlTask.class.getName());

  /**
   * Check function to determine whether algorithm has converged or not.
   * This is separate from the default stop condition,
   * which is based on the number of iterations made.
   */
  private final PageRankConvCond pageRankConvergenceCondition;

  /**
   * Damping Factor.
   */
  private final double dampingFactor;

  /**
   * Maximum number of iterations allowed before job stops.
   */
  private final int maxIter;

  /**
   * Map of <nodeid, rank>.
   */
  private Map<Integer, Double> rank;

  /**
   * Output stream provider to save the final ranks.
   */
  private final OutputStreamProvider outputStreamProvider;

  /**
   * Constructs the Controller Task for PageRank.
   * This class is instantiated by TANG.
   *
   * @param outputStreamProvider
   * @param maxIter maximum number of iterations allowed before job stops
   */
  @Inject
  public PageRankCtrlTask(final PageRankConvCond pageRankConvergenceCondition,
                          final OutputStreamProvider outputStreamProvider,
                          @Parameter(DampingFactor.class) final double dampingFactor,
                          @Parameter(MaxIterations.class) final int maxIter) {
    this.pageRankConvergenceCondition = pageRankConvergenceCondition;
    this.outputStreamProvider = outputStreamProvider;
    this.dampingFactor = dampingFactor;
    this.maxIter = maxIter;
    this.rank = new HashMap<>();
  }

  @Override
  public final void run(final int iteration) {
    LOG.log(Level.INFO, "{0}-th iteration", new Object[]{iteration});
  }

  @Override
  public final Map<Integer, Double> sendBroadcastData(final int iteration) {
    return rank;
  }

  @Override
  public final boolean isTerminated(final int iteration) {
    return pageRankConvergenceCondition.checkConvergence(rank)
        || iteration >= maxIter;
  }

  @Override
  public void receiveReduceData(final int iteration, final PageRankSummary increment) {
    rank = increment.getModel();
    for (final Integer key : rank.keySet()) {
      rank.put(key, (1 - dampingFactor) + dampingFactor * rank.get(key));
    }
  }

  @Override
  public void cleanup() {
    //output the ranks
    try (final DataOutputStream rankStream = outputStreamProvider.create("rank")) {
      rankStream.writeBytes(String.format("node_id,rank%n"));
      for (final Map.Entry<Integer, Double> entry : rank.entrySet()) {
        rankStream.writeBytes(String.format("%d,%f%n", entry.getKey(), entry.getValue()));
      }
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
