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

/**
 * Tree interface for diverse trees.
 *
 * Classes implementing this interface should include basic functions of trees, such as get(), leftChild(),
 * rightChild()...
 * All the trees in this folder is built with List<T> and the index of the list and the tree matches as follow.
 *            0
 *           / \
 *          1   2
 *         / \ / \
 *        3  4 5  6
 *        .. ... ..
 */
public interface Tree<T> {
  /**
   * Return the node at nodeIdx-index of the list.
   */
  T get(int nodeIdx);

  /**
   * Return the left child of the node at nodeIdx-index of the list.
   */
  T leftChild(int nodeIdx);

  /**
   * Return the right child of the node at nodeIdx-index of the list.
   */
  T rightChild(int nodeIdx);

  /**
   * Add newNode at the end of the list.
   */
  void add(T newNode);

  /**
   * Clear all the data in the list.
   */
  void clear();
}
