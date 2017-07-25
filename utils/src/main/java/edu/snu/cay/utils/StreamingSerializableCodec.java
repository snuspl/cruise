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
package edu.snu.cay.utils;

import org.apache.reef.io.network.impl.StreamingCodec;

import javax.inject.Inject;
import java.io.*;

/**
 * A {@link StreamingCodec} for {@link Serializable} objects.
 */
public final class StreamingSerializableCodec<T extends Serializable> implements StreamingCodec<T> {

  @Inject
  private StreamingSerializableCodec() {

  }

  @Override
  public void encodeToStream(final T obj, final DataOutputStream dataOutputStream) {
    try (ObjectOutputStream out = new ObjectOutputStream(dataOutputStream)) {
      out.writeObject(obj);
    } catch (IOException e) {
      throw new RuntimeException("Unable to encode: " + obj, e);
    }
  }

  @Override
  public T decodeFromStream(final DataInputStream dataInputStream) {
    try (ObjectInputStream in = new ObjectInputStream(dataInputStream)) {
      return (T) in.readObject();
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException("Unable to decode.", e);
    }
  }

  @Override
  public byte[] encode(final T obj) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
         ObjectOutputStream oos = new ObjectOutputStream(baos);
         DataOutputStream daos = new DataOutputStream(oos)) {
      encodeToStream(obj, daos);
      return baos.toByteArray();
    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }
  }

  @Override
  public T decode(final byte[] buf) {
    try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(buf))) {
      return decodeFromStream(dis);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
