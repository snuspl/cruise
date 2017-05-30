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
package edu.snu.cay.dolphin.async.optimizer.impl;

/**
 * This instance contains information about which evaluator should send or receive blocks and how many blocks are need
 * to be moved.
 */
final class BlockDelta {
  private int numBlocksToMove;
  private final int evalId;
  
  BlockDelta(final int numBlocksToMove, final int evalId) {
    this.numBlocksToMove = numBlocksToMove;
    this.evalId = evalId;
  }
  
  void setNumBlocksToMove(final int numBlocksToMove) {
    this.numBlocksToMove = numBlocksToMove;
  }
  
  int getNumBlocksToMove() {
    return numBlocksToMove;
  }
  
  int getEvalId() {
    return evalId;
  }
}
