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
 * This tree is for sortedByFeature list.
 * In each node of the tree, data are sorted by their feature values.
 */
public final class SortedTree implements Tree<List<Pair<Integer, Double>>> {

  private final int treeSize;
  private final List<List<Pair<Integer, Double>>> sortedTree;

  public SortedTree(final int treeMaxDepth) {
    this.treeSize = (1 << treeMaxDepth) - 1;
    this.sortedTree = new ArrayList<>(treeSize);
    for (int i = 0; i < treeSize; i++) {
      sortedTree.add(new ArrayList<>());
    }
  }

  @Override
  public List<Pair<Integer, Double>> get(final int thisNode) {
    return sortedTree.get(thisNode);
  }

  @Override
  public List<Pair<Integer, Double>> leftChild(final int thisNode) {
    return sortedTree.get(2 * thisNode + 1);
  }

  @Override
  public List<Pair<Integer, Double>> rightChild(final int thisNode) {
    return sortedTree.get(2 * thisNode + 2);
  }

  @Override
  public void add(final List<Pair<Integer, Double>> newNode) {
    sortedTree.add(newNode);
  }

  @Override
  public void clear() {
    for (int i = 0; i < treeSize; i++) {
      sortedTree.get(i).clear();
    }
    sortedTree.clear();
  }
}
