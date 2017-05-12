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

/**
 * Initialize the {@link PartitionStore} by using this.
 * It also maps the vertex id to graph partition index.
 */
public class GraphPartitioner {

  private final int numPartitions;

  public GraphPartitioner(final int numPartitions) {
    this.numPartitions = numPartitions;
  }

  public int getNumPartitions() {
    return numPartitions;
  }

  public int getPartitionIdx(final Integer vertexId) {
    return vertexId % numPartitions;
  }
}
