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

import java.util.ArrayList;
import java.util.List;

/**
 * Tree class for diverse trees.
 * Many trees(GBTree, DataTree, GroupedTree, SortedTree) extend this class.
 *
 * This tree class supports Complete Binary Tree.
 * Thus, {@code add(final T newNode)} function adds the {@code newNode} at the end of the tree list.
 */
public abstract class Tree<T> {
  private final int treeMaxDepth;
  protected final int treeSize;
  protected final List<T> tree;
  
  Tree(final int treeMaxDepth) {
    this.treeMaxDepth = treeMaxDepth;
    this.treeSize = (1 << treeMaxDepth) - 1;
    this.tree = new ArrayList<>(treeSize);
  }
  
  public int getDepth(final int nodeIdx) {
    int depth = treeMaxDepth;
    int fallingNodeIdx = nodeIdx;
    while (fallingNodeIdx < treeSize) {
      final int leftChild = 2 * fallingNodeIdx + 1;
      if (leftChild >= treeSize) {
        return depth;
      }
      fallingNodeIdx = leftChild;
      depth--;
    }
    return depth;
  }
  
  public T root() {
    return tree.get(0);
  }
  
  public T get(final int nodeIdx) {
    return tree.get(nodeIdx);
  }
  
  public T leftChild(final int nodeIdx) {
    return tree.get(2 * nodeIdx + 1);
  }
  
  public T rightChild(final int nodeIdx) {
    return tree.get(2 * nodeIdx + 2);
  }
  
  public void add(final T newNode) {
    tree.add(newNode);
  }
  
  public void clear() {
    tree.clear();
  }
}
