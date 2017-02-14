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

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * This is a tree for Gradient Boosting Tree.
 * Every run() iteration makes new GBTree(by buildTree()) and push the tree to the server.
 */
final class GBTree implements Tree<Pair<Integer, Double>> {

  private final List<Pair<Integer, Double>> gbTree;

  GBTree(final int treeMaxDepth) {
    gbTree = new ArrayList<>((1 << treeMaxDepth) - 1);
  }

  @Override
  public Pair<Integer, Double> get(final int thisNode) {
    return gbTree.get(thisNode);
  }

  @Override
  public Pair<Integer, Double> leftChild(final int thisNode) {
    return gbTree.get(2 * thisNode + 1);
  }

  @Override
  public Pair<Integer, Double> rightChild(final int thisNode) {
    return gbTree.get(2 * thisNode + 2);
  }

  @Override
  public void add(final Pair<Integer, Double> newNode) {
    gbTree.add(newNode);
  }

  @Override
  public void clear() {
    gbTree.clear();
  }
}
