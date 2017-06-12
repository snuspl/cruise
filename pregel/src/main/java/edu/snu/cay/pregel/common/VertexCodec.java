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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import edu.snu.cay.pregel.graph.api.Edge;
import edu.snu.cay.pregel.graph.api.Vertex;
import edu.snu.cay.pregel.graph.impl.DefaultVertex;
import edu.snu.cay.pregel.graph.impl.NoneValueEdge;
import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;
import java.io.*;
import java.util.List;
import java.util.logging.Logger;

/**
 * Codec for vertex.
 * Note that it assumes that vertex can be encoded
 * until the value of vertex is initialized.
 */
public final class VertexCodec<V> implements Codec<Vertex<V>> {

  private static final Logger LOG = Logger.getLogger(VertexCodec.class.getName());

  @Inject
  private VertexCodec() {
  }

  @Override
  public byte[] encode(final Vertex<V> vertex) {
    final Iterable<Edge> edges = vertex.getEdges();
    final ByteArrayOutputStream baos = new ByteArrayOutputStream(getNumBytes(edges));
    final DataOutputStream daos = new DataOutputStream(baos);
    try {
      daos.writeLong(vertex.getId());
      daos.writeInt(Iterables.size(edges));
      for (final Edge edge : edges) {
        daos.writeLong(edge.getTargetVertexId());
      }
    } catch (IOException e) {
      throw new RuntimeException("Could not serialize vertex");
    }

    return baos.toByteArray();
  }

  @Override
  public Vertex<V> decode(final byte[] bytes) {
    final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    final DataInputStream dais = new DataInputStream(bais);
    final Vertex<V> decodedVertex = new DefaultVertex<>();

    try {
      final Long vertexId = dais.readLong();
      final List<Edge> edges = Lists.newArrayList();
      final int edgesSize = dais.readInt();
      for (int index = 0; index < edgesSize; index++) {
        edges.add(new NoneValueEdge(dais.readLong()));
      }
      decodedVertex.initialize(vertexId, edges);
    } catch (IOException e) {
      throw new RuntimeException("Could not deserialize vertex");
    }
    return decodedVertex;
  }

  private int getNumBytes(final Iterable<Edge> edges) {
    final int size = Iterables.size(edges);
    return Long.BYTES + Integer.BYTES + Long.BYTES * size;
  }
}
