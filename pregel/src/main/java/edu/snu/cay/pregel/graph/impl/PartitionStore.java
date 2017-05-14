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
package edu.snu.cay.pregel.graph.impl;

import com.google.common.collect.Maps;
import edu.snu.cay.pregel.graph.api.Edge;
import edu.snu.cay.pregel.graph.api.Vertex;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * Assuming partition index is vertexId % the number of partitions.
 */
public class PartitionStore<V> {


  private final ConcurrentMap<Integer, Partition<V>> partitions = Maps.newConcurrentMap();

  public PartitionStore(final GraphPartitioner graphPartitioner,
                        final List<String> input) {

    input.forEach(line -> {
      final List<String> idStringList = Arrays.asList(line.split(" "));
      final List<Integer> idList = idStringList.stream().map(Integer::parseInt).collect(Collectors.toList());
      final Vertex<V> vertex = new DefaultVertex<>();
      final Integer vertexId = idList.remove(0);
      final List<Edge> edges = idList.stream().map(DefaultEdge::new).collect(Collectors.toList());
      vertex.initialize(vertexId, edges);

      final int partitionIdx = vertexId % graphPartitioner.getNumPartitions();
      partitions.putIfAbsent(partitionIdx, new Partition<>());
      partitions.get(partitionIdx).putVertex(vertex);
    });
  }

  public int getNumPartitions() {
    return partitions.entrySet().size();
  }

  public Partition<V> getPartition(final int partitionIdx) {
    if (!partitions.containsKey(partitionIdx)) {
      partitions.put(partitionIdx, new Partition<>());
    }
    return partitions.get(partitionIdx);
  }

}
