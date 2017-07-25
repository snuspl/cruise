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
package edu.snu.cay.pregel.graphapps.shortestpath;

import edu.snu.cay.pregel.graph.api.Edge;
import edu.snu.cay.pregel.graph.api.Vertex;
import edu.snu.cay.pregel.graph.impl.AbstractComputation;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.logging.Logger;

/**
 * Implementation of {@link edu.snu.cay.pregel.graph.api.Computation} to execute a pagerank algorithm.
 */
public final class ShortestPathComputation extends AbstractComputation<Long, Long, Long> {

  private static final Logger LOG = Logger.getLogger(ShortestPathComputation.class.getName());
  private final Long sourceId;

  @Inject
  private ShortestPathComputation(@Parameter(SourceId.class) final Long sourceId) {
    this.sourceId = sourceId;
  }

  @Override
  public void compute(final Vertex<Long, Long> vertex, final Iterable<Long> messages) {

    // initialize vertices
    if (getSuperstep() == 0) {
      vertex.setValue(Long.MAX_VALUE);
    }

    Long minDist = sourceId.equals(vertex.getId()) ? 0L : Long.MAX_VALUE;
    for (final Long message : messages) {
      if (message < minDist) {
        minDist = message;
      }
    }

    // if minimum distance value from messages is lower than the original value of vertex
    // update the new value
    if (minDist < vertex.getValue()) {
      vertex.setValue(minDist);
      for (final Edge<Long> edge : vertex.getEdges()) {
        final Long distance = minDist + edge.getValue();
        sendMessage(edge.getTargetVertexId(), distance);
      }
    }
    vertex.voteToHalt();
  }
}
