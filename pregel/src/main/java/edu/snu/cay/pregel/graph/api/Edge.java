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
package edu.snu.cay.pregel.graph.api;

/**
 * Interface for edge which has target vertex id.
 */
public interface Edge<V> {

  /**
   * Get the target vertex index of this edge.
   *
   * @return target vertex index of this edge
   */
  Long getTargetVertexId();

  /**
   * Get the edge value of the edge
   *
   * @return edge value of the edge
   */
  V getValue();
}
