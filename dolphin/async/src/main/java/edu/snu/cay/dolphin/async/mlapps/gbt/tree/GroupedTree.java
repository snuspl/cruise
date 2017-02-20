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

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * This tree is for groupedByLabel list.
 * In each node of the tree, data are grouped by their label values and save two values.
 * - the number of data in the label.
 * - sum of the g-values of the data in the label.
 *
 * First value of the pair is the number of values in that label.
 * Second value of the pair is the sum of the g values of the values that are belong to the label.
 */
public final class GroupedTree implements Tree<List<Pair<Integer, Double>>> {

  private final int treeSize;
  private final List<List<Pair<Integer, Double>>> groupedTree;

  public GroupedTree(final int treeMaxDepth) {
    this.treeSize = (1 << treeMaxDepth) - 1;
    this.groupedTree = new ArrayList<>(treeSize);
    for (int i = 0; i < treeSize; i++) {
      groupedTree.add(new ArrayList<>());
    }
  }

  @Override
  public List<Pair<Integer, Double>> get(final int thisNode) {
    return groupedTree.get(thisNode);
  }

  @Override
  public List<Pair<Integer, Double>> leftChild(final int thisNode) {
    return groupedTree.get(2 * thisNode + 1);
  }

  @Override
  public List<Pair<Integer, Double>> rightChild(final int thisNode) {
    return groupedTree.get(2 * thisNode + 2);
  }

  @Override
  public void add(final List<Pair<Integer, Double>> newNode) {
    groupedTree.add(newNode);
  }

  @Override
  public void clear() {
    for (int i = 0; i < treeSize; i++) {
      groupedTree.get(i).clear();
    }
    groupedTree.clear();
  }
}
