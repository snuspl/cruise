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

    final ByteArrayOutputStream var20 = new ByteArrayOutputStream(32 + totalLength);

    try {
      final DataOutputStream var21 = new DataOutputStream(var20);
      Throwable var5 = null;

      try {
        var21.writeInt(list.size());
        final Iterator var6 = list.iterator();

        while (var6.hasNext()) {
          final Vector vector = (Vector)var6.next();
          final int length = vector.length();
          var21.writeInt(length);

          for (int i = 0; i < length; ++i) {
            var21.writeDouble(vector.get(i));
          }
        }
      } catch (Throwable var17) {
        var5 = var17;
        throw var17;
      } finally {
        if (var21 != null) {
          if (var5 != null) {
            try {
              var21.close();
            } catch (Throwable var16) {
              var5.addSuppressed(var16);
            }
          } else {
            var21.close();
          }
        }

      }
    } catch (IOException var19) {
      throw new RuntimeException(var19.getCause());
    }

    return var20.toByteArray();
  }

  public List<Vector> decode(final byte[] data) {
    final ByteArrayInputStream bais = new ByteArrayInputStream(data);
    final LinkedList resultList = new LinkedList();

    try {
      final DataInputStream e = new DataInputStream(bais);
      Throwable var5 = null;

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
      } catch (Throwable var19) {
        var5 = var19;
        throw var19;
      } finally {
        if (e != null) {
          if (var5 != null) {
            try {
              e.close();
            } catch (Throwable var18) {
              var5.addSuppressed(var18);
            }
          } else {
            e.close();
          }
        }

      }

      return resultList;
    } catch (IOException var21) {
      throw new RuntimeException(var21.getCause());
    }
  }
}
