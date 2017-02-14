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
package edu.snu.cay.dolphin.async.mlapps.gbt;

import java.util.ArrayList;
import java.util.List;

/**
 * This tree saves data in each node.
 * At first, all the data are in the tree root.
 * As run() proceeds, each data goes to left or right child.
 * Then those data will be added in the left or right child node.
 */
final class DataTree implements Tree<List<Integer>> {

  private final int treeSize;
  private final List<List<Integer>> dataTree;

  DataTree(final int treeMaxDepth) {
    this.treeSize = powerTwo(treeMaxDepth) - 1;
    this.dataTree = new ArrayList<>(treeSize);
  }

  @Override
  public List<Integer> get(final int thisNode) {
    return dataTree.get(thisNode);
  }

  @Override
  public List<Integer> leftChild(final int thisNode) {
    return dataTree.get(2 * thisNode + 1);
  }

  @Override
  public List<Integer> rightChild(final int thisNode) {
    return dataTree.get(2 * thisNode + 2);
  }

  @Override
  public void add(final List<Integer> newNode) {
    dataTree.add(newNode);
  }

  @Override
  public void clear() {
    for (int i = 0; i < treeSize; i++) {
      dataTree.get(i).clear();
    }
    dataTree.clear();
  }

  private int powerTwo(final int p) {
    int ret = 1;
    for (int i = 0; i < p; i++) {
      ret *= 2;
    }
    return ret;
  }
}
