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
 * This tree saves data in each node.
 * At first, all the data are in the tree root.
 * As run() proceeds, each data goes to left or right child.
 * Then those data will be added in the left or right child node.
 */
public final class DataTree extends Tree<List<Integer>> {
  public DataTree(final int treeMaxDepth, final int dataSize) {
    super(treeMaxDepth);
    for (int i = 0; i < treeSize; i++) {
      tree.add(new ArrayList<>());
    }
    for (int i = 0; i < dataSize; i++) {
      this.root().add(i);
    }
  }

  @Override
  public void clear() {
    for (final List<Integer> node : tree) {
      node.clear();
    }
    tree.clear();
  }
}
