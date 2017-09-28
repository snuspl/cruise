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
import org.apache.commons.lang3.NotImplementedException;
import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.*;
import java.util.List;
import java.util.logging.Logger;

/**
 * Codec for {@link DefaultVertex}, which is composed of {@link VertexValueCodec} and {@link EdgeCodec}.
 *
 * Encoding format of vertex is as follows:
 * 1. Vertex w/ value : [ long: vertex id | true | V: vertex value | E: edge | E: edge | E: edge ...],
 * 2. Vertex w/o value : [ long: vertex id | false | E: edge | E: edge | E: edge ...]
 *
 * Encoding format of edge is as follows:
 * 1. Edge w/ value : [ long: target vertex id | E: edge value ]
 * 2. Edge w/o value : [ long: target vertex id ]
 */
public final class DefaultVertexCodec<V, E> implements Codec<Vertex<V, E>>, StreamingCodec<Vertex<V, E>> {

  private static final Logger LOG = Logger.getLogger(DefaultVertexCodec.class.getName());
  private final StreamingCodec<V> vertexValueCodec;
  private final StreamingCodec<Edge<E>> edgeCodec;

  @Inject
  private DefaultVertexCodec(@Parameter(VertexValueCodec.class) final StreamingCodec<V> vertexValueCodec,
                             @Parameter(EdgeCodec.class) final StreamingCodec<Edge<E>> edgeCodec) {
    this.vertexValueCodec = vertexValueCodec;
    this.edgeCodec = edgeCodec;
  }

  @Override
  public byte[] encode(final Vertex<V, E> vertex) {
    final Iterable<Edge<E>> edges = vertex.getEdges();
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
         DataOutputStream daos = new DataOutputStream(baos)) {
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
      return baos.toByteArray();

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Vertex<V, E> decode(final byte[] bytes) {
    final Vertex<V, E> decodedVertex = new DefaultVertex<>();
    try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
         DataInputStream dais = new DataInputStream(bais)) {
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
      return decodedVertex;

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void encodeToStream(final Vertex<V, E> obj, final DataOutputStream stream) {
    throw new NotImplementedException("not implemented exception");
  }

  @Override
  public Vertex<V, E> decodeFromStream(final DataInputStream stream) {
    throw new NotImplementedException("not implemented exception");
  }
}
