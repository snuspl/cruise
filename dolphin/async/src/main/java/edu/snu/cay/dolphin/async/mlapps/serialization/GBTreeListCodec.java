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
import java.util.LinkedList;
import java.util.List;

/**
 * Codec for GBTree list.
 */
public final class GBTreeListCodec implements Codec<List<GBTree>> {

  @Inject
  private GBTreeListCodec() {
  }

  public byte[] encode(final List<GBTree> gbTreeList) {

    // This codec assumes that lists have the same length
    int treeMaxDepth = 0;
    int treeSize = 0;
    for (final GBTree gbTree : gbTreeList) {
      treeMaxDepth = gbTree.getMaxDepth();
      treeSize = gbTree.getTreeSize();
    }

    // (1) the size of the GBTree list, (2) the max depth of GBTree, and (3) the all the GBTree's size.
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream(Integer.SIZE + Integer.SIZE +
        (Integer.SIZE + Double.SIZE) * treeSize)) {
      try (DataOutputStream dos = new DataOutputStream(baos)) {
        dos.writeInt(gbTreeList.size());
        dos.writeInt(treeMaxDepth);

        for (final GBTree gbTree : gbTreeList) {
          for (final Pair<Integer, Double> node : gbTree.getTree()) {
            dos.writeInt(node.getLeft());
            dos.writeDouble(node.getRight());
          }
        }
      }
      return baos.toByteArray();
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  public List<GBTree> decode(final byte[] data) {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(data)) {
      final List<GBTree> resultList = new LinkedList<>();

      try (DataInputStream dais = new DataInputStream(bais)) {
        final int listSize = dais.readInt();
        final int treeMaxDepth = dais.readInt();
        for (int i = 0; i < listSize; ++i) {
          final GBTree gbTree = new GBTree(treeMaxDepth);
          for (int j = 0; j < (1 << treeMaxDepth) - 1; j++) {
            final int splitFeature = dais.readInt();
            final double splitValue = dais.readDouble();
            gbTree.add(Pair.of(splitFeature, splitValue));
          }
          resultList.add(gbTree);
        }
      }
      return resultList;
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
