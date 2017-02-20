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
package edu.snu.cay.dolphin.async.mlapps.gbt.tree;

import edu.snu.cay.dolphin.async.mlapps.gbt.NodeState;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

/**
 * This is a tree for Gradient Boosting Tree.
 * Every run() iteration makes new GBTree(by buildTree()) and push the tree to the server.
 */
public final class GBTree extends Tree<Pair<Integer, Double>> {
  public GBTree(final int treeMaxDepth) {
    super(treeMaxDepth);
  }
  
  public void makeLeaf(final int nodeIdx, final DataTree dataTree, final List<Double> gValues,
                       final double lambda) {
    double gSum = 0;
    final List<Integer> thisNode = dataTree.get(nodeIdx);
    for (final int leafMember : thisNode) {
      gSum += gValues.get(leafMember);
    }
    tree.add(Pair.of(NodeState.LEAF.getValue(), -gSum / (2 * thisNode.size() + lambda)));
  }
}
