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
package edu.snu.cay.dolphin.async.optimizer.impl.ilp;

/**
 * This instance contains information about which evaluator should send or receive blocks and how many blocks are need
 * to be moved.
 * If the evaluator(with {@code evalIdx})'s role is a sender, {@code numBlocksToMove} is defined as a number of blocks
 * to send. If the evaluator's role is a receiver, {@code numBlocksToMove} is defined as a number of blocks to receive.
 * Thus, the value is always positive.
 */
final class BlockDelta {
  /**
   * Number of blocks that evaluator with {@code evalIdx} sends or receives.
   */
  private int numBlocksToMove;
  private final int evalIdx;
  
  BlockDelta(final int numBlocksToMove, final int evalIdx) {
    this.numBlocksToMove = numBlocksToMove;
    this.evalIdx = evalIdx;
  }
  
  void setNumBlocksToMove(final int numBlocksToMove) {
    this.numBlocksToMove = numBlocksToMove;
  }
  
  int getNumBlocksToMove() {
    return numBlocksToMove;
  }
  
  int getEvalIdx() {
    return evalIdx;
  }
}
