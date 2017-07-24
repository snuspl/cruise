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
 * Created by cmslab on 7/21/17.
 */
public final class DefaultEdgeCodec<T extends Serializable> implements StreamingCodec<Edge<T>> {

  private StreamingSerializableCodec<T> edgeValueCodec;
  @Inject
  private DefaultEdgeCodec(final StreamingSerializableCodec<T> edgeValueCodec) {
    this.edgeValueCodec = edgeValueCodec;
  }

  @Override
  public byte[] encode(final Edge<T> edge) {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream(Long.BYTES);
    final DataOutputStream daos = new DataOutputStream(baos);

    try {
      daos.writeLong(edge.getTargetVertexId());
      daos.write(edgeValueCodec.encode(edge.getValue()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return baos.toByteArray();
  }

  @Override
  public Edge<T> decode(final byte[] bytes) {
    final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    final DataInputStream dais = new DataInputStream(bais);
    try {
      final Long targetVertexId = dais.readLong();
      final T value = edgeValueCodec.decodeFromStream(dais);
      return new DefaultEdge<>(targetVertexId, value);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void encodeToStream(final Edge<T> edge, final DataOutputStream dataOutputStream) {
    try {
      dataOutputStream.writeLong(edge.getTargetVertexId());
      edgeValueCodec.encodeToStream(edge.getValue(), dataOutputStream);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Edge<T> decodeFromStream(final DataInputStream dataInputStream) {
    try {
      final long targetVertexId = dataInputStream.readLong();
      final T value = edgeValueCodec.decodeFromStream(dataInputStream);
      return new DefaultEdge<>(targetVertexId, value);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
