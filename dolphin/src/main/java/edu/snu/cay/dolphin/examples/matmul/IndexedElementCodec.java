/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.cay.dolphin.examples.matmul;

import org.apache.reef.wake.remote.Codec;

import javax.inject.Inject;
import java.io.*;

public final class IndexedElementCodec implements Codec<IndexedElement> {

  @Inject
  private IndexedElementCodec() {
  }

  @Override
  public IndexedElement decode(final byte[] bytes) {
    final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    try (final DataInputStream dais = new DataInputStream(bais)) {
      final int row = dais.readInt();
      final int column = dais.readInt();
      final double value = dais.readDouble();

      return new IndexedElement(row, column, value);
    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }
  }

  @Override
  public byte[] encode(final IndexedElement indexedElement) {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream(Integer.SIZE * 2 + Double.SIZE);
    try (final DataOutputStream daos = new DataOutputStream(baos)) {
      daos.writeInt(indexedElement.getRow());
      daos.writeInt(indexedElement.getColumn());
      daos.writeDouble(indexedElement.getValue());
    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }

    return baos.toByteArray();
  }
}
