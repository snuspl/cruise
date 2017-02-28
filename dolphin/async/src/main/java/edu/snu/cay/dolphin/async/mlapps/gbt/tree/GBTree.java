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
 *
 * Each node consists of a pair of two values: the first is the index of the criterion feature
 * and the second is the value to split the node.
 * (e.g., For a node (3,7), data falls into the left child if the value is smaller than 7 at feature 3.
 * it goes to the right child otherwise)
 *
 * If the criterion feature is numerical type, split point is real number.
 *  - If a data's criterion feature value is less than or equal to the split point, the data goes to the left child.
 *  - If a data's criterion feature value is greater than the split point, the data goes to the right child.)
 * If the criterion feature is categorical type, split point is binary number.
 *  - Label order is descending order from the higher digit number.
 *  - 0 indicates left child while 1 indicates right child.
 * ex) 10011 : if the feature's label is 2, 3, the data goes to left child.
 *             if the feature's label is 0, 1, 4, the data goes to the right child.
 *
 * If the first instance is NodeState.EMPTY(-2), it means the tree node does not exist.
 * If the first instance is NodeState.LEAF(-1), it means the tree node is a leaf node.
 */
public final class GBTree extends Tree<Pair<Integer, Double>> {
  public GBTree(final int treeMaxDepth) {
    super(treeMaxDepth);
  }
  
  /**
   * Make a node into a leaf node.
   *
   * @param nodeIdx Make nodeIdx-index node into leaf node.
   * @param dataTree dataTree's data is needed to compute leaf weight.
   * @param gValues gValues's data is needed to compute leaf weight.
   * @param lambda lambda is needed to compute leaf weight.
   */
  public void makeLeaf(final int nodeIdx, final DataTree dataTree, final List<Double> gValues,
                       final double lambda) {
    double gSum = 0;
    final List<Integer> thisNode = dataTree.get(nodeIdx);
    for (final int leafMember : thisNode) {
      gSum += gValues.get(leafMember);
    }
    tree.add(Pair.of(NodeState.LEAF.getValue(), -gSum / (2 * thisNode.size() + lambda)));
  }

  /**
   * @return max depth of the tree.
   */
  public int getMaxDepth() {
    return this.treeMaxDepth;
  }

  /**
   * @return size of the tree.
   */
  public int getTreeSize() {
    return this.treeSize;
  }

  /**
   * @return tree list.
   */
  public List<Pair<Integer, Double>> getTree() {
    return this.tree;
  }
}
