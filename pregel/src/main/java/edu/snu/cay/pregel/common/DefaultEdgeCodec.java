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

import edu.snu.cay.pregel.graph.api.Edge;
import edu.snu.cay.pregel.graph.impl.DefaultEdge;
import edu.snu.cay.utils.StreamingSerializableCodec;
import org.apache.reef.io.network.impl.StreamingCodec;

import javax.inject.Inject;
import java.io.*;

/**
 * Codec for a edge.
 * Assume that type of edge value is {@link Serializable}.
 */
public final class DefaultEdgeCodec<E extends Serializable> implements StreamingCodec<Edge<E>> {

  private final StreamingSerializableCodec<E> edgeValueCodec;
  
  @Inject
  private DefaultEdgeCodec(final StreamingSerializableCodec<E> edgeValueCodec) {
    this.edgeValueCodec = edgeValueCodec;
  }

  @Override
  public byte[] encode(final Edge<E> edge) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
         DataOutputStream daos = new DataOutputStream(baos)) {
      daos.writeLong(edge.getTargetVertexId());
      daos.write(edgeValueCodec.encode(edge.getValue()));
      return baos.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Edge<E> decode(final byte[] bytes) {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
         DataInputStream dais = new DataInputStream(bais)) {
      final Long targetVertexId = dais.readLong();
      final E value = edgeValueCodec.decodeFromStream(dais);
      return new DefaultEdge<>(targetVertexId, value);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void encodeToStream(final Edge<E> edge, final DataOutputStream dataOutputStream) {
    try {
      dataOutputStream.writeLong(edge.getTargetVertexId());
      edgeValueCodec.encodeToStream(edge.getValue(), dataOutputStream);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Edge<E> decodeFromStream(final DataInputStream dataInputStream) {
    try {
      final long targetVertexId = dataInputStream.readLong();
      final E value = edgeValueCodec.decodeFromStream(dataInputStream);
      return new DefaultEdge<>(targetVertexId, value);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
