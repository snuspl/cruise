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
package edu.snu.cay.dolphin.async.mlapps.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;
import javax.inject.Inject;

import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.common.math.linalg.VectorFactory;
import org.apache.reef.io.serialization.Codec;

/**
 * Codec for vector list.
 */
public final class VectorListCodec implements Codec<List<Vector>> {

  private final VectorFactory vectorFactory;

  @Inject
  public VectorListCodec(final VectorFactory vectorFactory) {
    this.vectorFactory = vectorFactory;
  }

  public byte[] encode(final List<Vector> list) {
    int totalLength = 0;

    Vector e;
    for (Iterator baos = list.iterator(); baos.hasNext();) {
      e = (Vector)baos.next();
      totalLength += 32 + 64 * e.length();
    }

    final ByteArrayOutputStream baos = new ByteArrayOutputStream(Integer.SIZE + totalLength);

    try (DataOutputStream dos  = new DataOutputStream(baos)) {
      Throwable totalThrow = null;

      try {
        dos.writeInt(list.size());
        final Iterator dataFromList = list.iterator();

        while (dataFromList.hasNext()) {
          final Vector vector = (Vector)dataFromList.next();
          final int length = vector.length();
          dos.writeInt(length);

          for (int i = 0; i < length; ++i) {
            dos.writeDouble(vector.get(i));
          }
        }
      } catch (Throwable throw1) {
        totalThrow = throw1;
        throw throw1;
      } finally {
        if (dos != null) {
          if (totalThrow != null) {
            try {
              dos.close();
            } catch (Throwable throw2) {
              totalThrow.addSuppressed(throw2);
            }
          } else {
            dos.close();
          }
        }

      }
    } catch (IOException ioException) {
      throw new RuntimeException(ioException.getCause());
    }

    return baos.toByteArray();
  }

  public List<Vector> decode(final byte[] data) {
    final ByteArrayInputStream bais = new ByteArrayInputStream(data);
    final LinkedList resultList = new LinkedList();

    try {
      final DataInputStream e = new DataInputStream(bais);
      Throwable totalThrow = null;

      try {
        final int listSize = e.readInt();

        for (int i = 0; i < listSize; ++i) {
          final int length = e.readInt();
          final Vector vector = vectorFactory.createDenseZeros(length);

          for (int j = 0; j < length; ++j) {
            vector.set(j, e.readDouble());
          }

          resultList.add(vector);
        }
      } catch (Throwable throw1) {
        totalThrow = throw1;
        throw throw1;
      } finally {
        if (e != null) {
          if (totalThrow != null) {
            try {
              e.close();
            } catch (Throwable throw2) {
              totalThrow.addSuppressed(throw2);
            }
          } else {
            e.close();
          }
        }

      }

      return resultList;
    } catch (IOException ioException) {
      throw new RuntimeException(ioException.getCause());
    }
  }
}
