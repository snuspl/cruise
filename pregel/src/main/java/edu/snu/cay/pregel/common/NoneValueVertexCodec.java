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
import edu.snu.cay.pregel.PregelParameters.EdgeCodec;
import edu.snu.cay.pregel.graph.api.Edge;
import edu.snu.cay.pregel.graph.api.Vertex;
import edu.snu.cay.pregel.graph.impl.*;
import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.*;
import java.util.List;
import java.util.logging.Logger;

/**
 * Codec for {@link NoneValueVertex}.
 */
public final class NoneValueVertexCodec<E> implements Codec<Vertex<Void, E>> {

  private static final Logger LOG = Logger.getLogger(NoneValueVertexCodec.class.getName());
  private final StreamingCodec<Edge<E>> edgeCodec;

  @Inject
  private NoneValueVertexCodec(@Parameter(EdgeCodec.class) final StreamingCodec<Edge<E>> edgeCodec) {
    this.edgeCodec = edgeCodec;
  }

  @Override
  public byte[] encode(final Vertex<Void, E> vertex) {
    final Iterable<Edge<E>> edges = vertex.getEdges();
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
         DataOutputStream daos = new DataOutputStream(baos)) {
      daos.writeLong(vertex.getId());
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
  public Vertex<Void, E> decode(final byte[] bytes) {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
         DataInputStream dais = new DataInputStream(bais)) {
      final Vertex<Void, E> decodedVertex = new NoneValueVertex<>();

      final Long vertexId = dais.readLong();
      final List<Edge<E>> edges = Lists.newArrayList();
      final int edgesSize = dais.readInt();
      for (int index = 0; index < edgesSize; index++) {
        edges.add(edgeCodec.decodeFromStream(dais));
      }

      decodedVertex.initialize(vertexId, edges);
      return decodedVertex;

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
