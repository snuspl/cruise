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

import edu.snu.cay.pregel.graph.parameters.NumPartitions;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * Initialize the {@link PartitionStore} by using this.
 * It also maps the vertex id to graph partition index.
 */
public final class GraphPartitioner {

  private final int numPartitions;

  @Inject
  private GraphPartitioner(@Parameter(NumPartitions.class) final int numPartitions) {
    this.numPartitions = numPartitions;
  }

  /**
   * Get the number of partitions in this worker.
   *
   * @return the number of partitions in this worker
   */
  public int getNumPartitions() {
    return numPartitions;
  }

  /**
   * Get the index of partition from a vertex id.
   *
   * @param vertexId vertex id to look for
   * @return partition id from a vertex id
   */
  public int getPartitionIdx(final Long vertexId) {
    return (int)(long)vertexId % numPartitions;
  }
}
