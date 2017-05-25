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

import edu.snu.cay.dolphin.async.mlapps.gbt.tree.GBTree;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;
import java.io.*;

/**
 * Codec for GBTree.
 */
public final class GBTreeCodec implements Codec<GBTree> {

  @Inject
  private GBTreeCodec() {
  }

  public byte[] encode(final GBTree gbTree) {

    // (1) max depth of the gbTree, and (2) the sum of all inner node's size.
    try (ByteArrayOutputStream baos =
             new ByteArrayOutputStream(Integer.SIZE + (Integer.SIZE + Float.SIZE) * gbTree.getTreeSize())) {
      try (DataOutputStream dos = new DataOutputStream(baos)) {
        dos.writeInt(gbTree.getMaxDepth());

        for (final Pair<Integer, Float> node : gbTree.getTree()) {
          dos.writeInt(node.getLeft());
          dos.writeFloat(node.getRight());
        }
      }
      return baos.toByteArray();
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  public GBTree decode(final byte[] data) {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(data)) {
      final GBTree gbTree;

      try (DataInputStream dais = new DataInputStream(bais)) {
        final int treeMaxDepth = dais.readInt();
        gbTree = new GBTree(treeMaxDepth);

        for (int i = 0; i < (1 << treeMaxDepth) - 1; ++i) {
          final int splitFeature = dais.readInt();
          final Float splitValue = dais.readFloat();
          gbTree.add(Pair.of(splitFeature, splitValue));
        }
      }
      return gbTree;
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
