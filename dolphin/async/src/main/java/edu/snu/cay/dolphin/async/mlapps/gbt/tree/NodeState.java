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
 * This indicates each GBTree node's state.
 * If the node is empty, the node's state is EMPTY(-2).
 * If the node is leaf, the node's state is LEAF(-1).
 * Two NodeStates(EMPTY, LEAF) are assigned to integer values because they are written in the GBTree nodes'
 * first value(Integer) of the Pair.
 */
public enum NodeState {
  EMPTY(-2), LEAF(-1);
  
  private final int value;
  
  NodeState(final int value) {
    this.value = value;
  }
  
  public int getValue() {
    return value;
  }
}
