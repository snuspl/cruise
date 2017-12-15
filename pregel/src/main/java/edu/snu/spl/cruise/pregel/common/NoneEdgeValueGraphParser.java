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
package edu.snu.spl.cruise.pregel.common;

import com.google.common.collect.Lists;
import edu.snu.spl.cruise.pregel.graph.api.Edge;
import edu.snu.spl.cruise.pregel.graph.api.Vertex;
import edu.snu.spl.cruise.pregel.graph.impl.DefaultVertex;
import edu.snu.spl.cruise.pregel.graph.impl.NoneValueEdge;
import edu.snu.spl.cruise.services.et.evaluator.api.DataParser;
import org.apache.commons.lang3.tuple.Pair;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Data parser class for vertex id, adjacent vertex ids set.
 * Note that it doesn't have the value of edges.
 */
public final class NoneEdgeValueGraphParser<V> implements DataParser<Pair<Long, Vertex<V, Void>>> {

  @Inject
  private NoneEdgeValueGraphParser() {

  }

  @Override
  public List<Pair<Long, Vertex<V, Void>>> parse(final Collection<String> collection) {

    final List<Pair<Long, Vertex<V, Void>>> parsedList = new ArrayList<>();

    for (final String line : collection) {

      if (line.startsWith("#") || line.length() == 0) {
        continue;
      }

      final List<String> lineDatas = Lists.newArrayList(line.split(" "));
      final Long vertexId = Long.parseLong(lineDatas.get(0));
      final List<Edge<Void>> adjacentIds = lineDatas.subList(1, lineDatas.size()).stream().
          map(id -> new NoneValueEdge(Long.parseLong(id))).collect(Collectors.toList());
      final Vertex<V, Void> vertex = new DefaultVertex<>();
      vertex.initialize(vertexId, adjacentIds);
      parsedList.add(Pair.of(vertexId, vertex));
    }

    return parsedList;
  }
}
