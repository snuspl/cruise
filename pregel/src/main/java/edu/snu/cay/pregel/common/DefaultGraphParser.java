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
package edu.snu.cay.pregel.common;

import com.google.common.collect.Lists;
import edu.snu.cay.pregel.graph.api.Edge;
import edu.snu.cay.pregel.graph.api.Vertex;
import edu.snu.cay.pregel.graph.impl.DefaultEdge;
import edu.snu.cay.pregel.graph.impl.DefaultVertex;
import edu.snu.cay.services.et.evaluator.api.DataParser;
import org.apache.commons.lang3.tuple.Pair;

import javax.inject.Inject;
import java.util.*;

/**
 * Default Data parser class for vertex id, adjacent vertex id and distance set.
 */
public final class DefaultGraphParser<V, E> implements DataParser<Pair<Long, Vertex<V, E>>> {

  @Inject
  private DefaultGraphParser() {
  }

  @Override
  public List<Pair<Long, Vertex<V, E>>> parse(final Collection<String> collection) {

    final List<Pair<Long, Vertex<V, E>>> parsedList = new ArrayList<>();
    final Map<Long, List<Edge<E>>> parsedVertexIdEdgesMap = new HashMap<>();

    for (final String line : collection) {

      if (line.startsWith("#") || line.length() == 0) {
        continue;
      }

      // graph parser create a vertex by each line
      final List<String> lineDataList = Lists.newArrayList(line.split(" "));
      final Long vertexId = Long.parseLong(lineDataList.get(0));
      for (int i = 1; i < lineDataList.size() - 1; i += 2) {
        final Long targetVertexId = Long.parseLong(lineDataList.get(i));
        final Long edgeValue = Long.parseLong(lineDataList.get(i + 1));
        final Edge<E> edge = new DefaultEdge<>(targetVertexId, (E)edgeValue);
        parsedVertexIdEdgesMap.putIfAbsent(vertexId, new ArrayList<>());
        parsedVertexIdEdgesMap.get(vertexId).add(edge);
      }
    }

    parsedVertexIdEdgesMap.forEach((vertexId, edges) -> {
      final DefaultVertex<V, E> vertex = new DefaultVertex<>();
      vertex.initialize(vertexId, edges);
      parsedList.add(Pair.of(vertexId, vertex));
    });

    return parsedList;
  }
}
