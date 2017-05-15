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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

/**
 * Structure that stores partitions for worker.
 * Assuming partition index is vertexId % the number of partitions.
 */
public class PartitionStore<V> {


  private final ConcurrentMap<Integer, Partition<V>> partitions = Maps.newConcurrentMap();

  private final BlockingQueue<Partition<V>> partitionQueue = new LinkedBlockingQueue<>();

  public PartitionStore(final GraphPartitioner graphPartitioner,
                        final List<String> input) {

    // initialize the partition store
    input.forEach(line -> {
      final List<String> idStringList = Arrays.asList(line.split(" "));
      final List<Integer> idList = idStringList.stream().map(Integer::parseInt).collect(Collectors.toList());
      final Vertex<V> vertex = new DefaultVertex<>();
      final Integer vertexId = idList.remove(0);
      final List<Edge> edges = idList.stream().map(NoneValueEdge::new).collect(Collectors.toList());
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

  /**
   * Start the iteration cycle to iterate over partitions.
   * After an iteration is started, multiple threads can access the partition store
   * using {@link #getNextPartition()} to iterate over the partitions.
   */
  public void startIteration() {
    if (!partitionQueue.isEmpty()) {
      throw new RuntimeException("Partition Queue must be empty");
    }

    for (final Partition<V> partition : partitions.values()) {
      partitionQueue.add(partition);
    }
  }

  /**
   * Get the next partition in iteration for the current superstep.
   * The method {@link #startIteration()} must be called before call this.
   * if partition value is null, each thread breaks the loop and prepare for finishing current superstep.
   *
   * @return the next partition to process
   */
  public Partition<V> getNextPartition() {
    return partitionQueue.poll();
  }


}
