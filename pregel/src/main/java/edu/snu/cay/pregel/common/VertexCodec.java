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
import edu.snu.cay.pregel.PregelParameters.*;
import edu.snu.cay.pregel.graph.api.Edge;
import edu.snu.cay.pregel.graph.api.Vertex;
import edu.snu.cay.pregel.graph.impl.DefaultVertex;
import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.*;
import java.util.List;
import java.util.logging.Logger;

/**
 * Codec for vertex, is composed of {@link VertexValueCodec} and {@link EdgeCodec}.
 * So they should be configured before {@link edu.snu.cay.pregel.PregelWorkerTask} starts.
 *
 * Encoding format of vertex is as follows:
 * 1. Vertex w/ value : [ long: vertex id | true | V: vertex value | E: edge | E: edge | E: edge ...],
 * 2. Vertex w/o value : [ long: vertex id | false | E: edge | E: edge | E: edge ...]
 *
 * Encoding format of edge is as follows:
 * 1. Edge w/ value : [ long: target vertex id | E: edge value ]
 * 2. Edge w/o value : [ long: target vertex id ]
 */
public final class VertexCodec<V, E> implements Codec<Vertex<V, E>> {

  private static final Logger LOG = Logger.getLogger(VertexCodec.class.getName());
  private final StreamingCodec<V> vertexValueCodec;
  private final StreamingCodec<Edge<E>> edgeCodec;

  @Inject
  private VertexCodec(@Parameter(VertexValueCodec.class) final StreamingCodec<V> vertexValueCodec,
                      @Parameter(EdgeCodec.class) final StreamingCodec<Edge<E>> edgeCodec) {
    this.vertexValueCodec = vertexValueCodec;
    this.edgeCodec = edgeCodec;
  }

  @Override
  public byte[] encode(final Vertex<V, E> vertex) {
    final Iterable<Edge<E>> edges = vertex.getEdges();
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final DataOutputStream daos = new DataOutputStream(baos);
    try {
      daos.writeLong(vertex.getId());
      final V vertexValue = vertex.getValue();
      if (vertexValue != null) {
        daos.writeBoolean(true);
        daos.write(vertexValueCodec.encode(vertex.getValue()));
      } else {
        daos.writeBoolean(false);
      }
      daos.writeInt(Iterables.size(edges));
      for (final Edge<E> edge : edges) {
        daos.write(edgeCodec.encode(edge));
      }

    } catch (IOException e) {
      throw new RuntimeException("Could not serialize vertex");
    }
    return baos.toByteArray();
  }

  @Override
  public Vertex<V, E> decode(final byte[] bytes) {
    final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    final DataInputStream dais = new DataInputStream(bais);
    final Vertex<V, E> decodedVertex = new DefaultVertex<>();

    try {
      final Long vertexId = dais.readLong();
      final boolean isExistValue = dais.readBoolean();
      V vertexValue = null;
      if (isExistValue) {
        vertexValue = vertexValueCodec.decodeFromStream(dais);
      }
      final List<Edge<E>> edges = Lists.newArrayList();
      final int edgesSize = dais.readInt();
      for (int index = 0; index < edgesSize; index++) {
        edges.add(edgeCodec.decodeFromStream(dais));
      }

      decodedVertex.initialize(vertexId, vertexValue, edges);
    } catch (IOException e) {
      throw new RuntimeException("Could not deserialize vertex");
    }
    return decodedVertex;
  }
}
