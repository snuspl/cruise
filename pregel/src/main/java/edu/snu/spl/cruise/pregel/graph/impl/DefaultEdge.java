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
package edu.snu.spl.cruise.pregel.graph.impl;

import edu.snu.spl.cruise.pregel.graph.api.Edge;
import edu.snu.spl.cruise.pregel.graph.api.MutableEdge;

/**
 * A default implementation of {@link Edge}.
 *
 * @param <E> edge value
 */
public final class DefaultEdge<E> implements MutableEdge<E> {

  private final Long vertexId;
  private E value;

  public DefaultEdge(final Long vertexId, final E value) {
    this.vertexId = vertexId;
    this.value = value;
  }

  @Override
  public Long getTargetVertexId() {
    return vertexId;
  }

  @Override
  public E getValue() {
    return value;
  }

  @Override
  public void setValue(final E newValue) {
    this.value = newValue;
  }
}
