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
import edu.snu.cay.pregel.graph.impl.NoneValueEdge;
import org.apache.reef.io.network.impl.StreamingCodec;

import javax.inject.Inject;
import java.io.*;

/**
 * Codec for a edge.
 * Note that type of edge value is {@link Void}.
 */
public final class NoneValueEdgeCodec implements StreamingCodec<Edge<Void>> {

  @Inject
  private NoneValueEdgeCodec() {

  }

  @Override
  public byte[] encode(final Edge<Void> edge) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream(Long.BYTES);
         DataOutputStream daos = new DataOutputStream(baos)) {
      daos.writeLong(edge.getTargetVertexId());
      return baos.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Edge<Void> decode(final byte[] bytes) {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
         DataInputStream dais = new DataInputStream(bais)) {
      final Long targetVertexId = dais.readLong();
      return new NoneValueEdge(targetVertexId);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void encodeToStream(final Edge<Void> edge, final DataOutputStream dataOutputStream) {
    try {
      dataOutputStream.writeLong(edge.getTargetVertexId());
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Edge<Void> decodeFromStream(final DataInputStream dataInputStream) {
    try {
      final long targetVertexId = dataInputStream.readLong();
      return new NoneValueEdge(targetVertexId);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
