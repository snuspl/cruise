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

import edu.snu.cay.dolphin.async.optimizer.parameters.Constants;
import edu.snu.cay.dolphin.async.plan.impl.ILPPlanDescriptor;
import edu.snu.cay.dolphin.async.plan.impl.ILPTransferStepImpl;

import java.util.Comparator;
import java.util.PriorityQueue;

public final class ILPPlanGenerator {
  
  private static final int BLOCK_SIZE = 16;
  
  private ILPPlanDescriptor generatePlanDescriptor(final int[] oldRole, final int[] oldD, final int[] oldM,
                                                   final int[] newRole, final int[] newD, final int[] newM) {
    // 1.
    final int numTotalEval = oldRole.length;
    final ILPPlanDescriptor.Builder planBuilder = ILPPlanDescriptor.newBuilder();
    
    // check whether each evaluator's role is changed or not
    for (int i = 0; i < numTotalEval; i++) {
      if (oldRole[i] != newRole[i]) {
        if (oldRole[i] == 0) {
          // this evaluator is changed from worker to server
          planBuilder.addEvaluatorToAdd(Constants.NAMESPACE_SERVER, i);
          planBuilder.addEvaluatorToDelete(Constants.NAMESPACE_WORKER, i);
        } else {
          // this evaluator is changed from server to worker
          planBuilder.addEvaluatorToAdd(Constants.NAMESPACE_WORKER, i);
          planBuilder.addEvaluatorToDelete(Constants.NAMESPACE_SERVER, i);
        }
      }
    }
    
    // generate transfer plans for worker
    generateTransferPlans(Constants.NAMESPACE_WORKER, oldD, newD, planBuilder);
    // generate transfer plans for server
    generateTransferPlans(Constants.NAMESPACE_SERVER, oldM, newM, planBuilder);
    
    return planBuilder.build();
  }
  
  private void generateTransferPlans(final String namespace, final int[] oldState, final int[] newState,
                                     final ILPPlanDescriptor.Builder planBuilder) {
    final int numTotalEval = oldState.length;
    final PriorityQueue<BlockDelta> senderPriorityQueue =
        new PriorityQueue<>(numTotalEval, NUM_BLOCKS_TO_MOVE_COMPARATOR);
    final PriorityQueue<BlockDelta> receiverPriorityQueue =
        new PriorityQueue<>(numTotalEval, NUM_BLOCKS_TO_MOVE_COMPARATOR);
    for (int i = 0; i < numTotalEval; i++) {
      final int numBlocksToMove = oldState[i] / BLOCK_SIZE - newState[i] / BLOCK_SIZE;
      if (numBlocksToMove > 0) {
        senderPriorityQueue.add(new BlockDelta(numBlocksToMove, i));
      } else if (numBlocksToMove < 0) {
        receiverPriorityQueue.add(new BlockDelta(-numBlocksToMove, i));
      }
    }
    
    while (!senderPriorityQueue.isEmpty() && !receiverPriorityQueue.isEmpty()) {
      final BlockDelta sender = senderPriorityQueue.poll();
      final BlockDelta receiver = receiverPriorityQueue.poll();
      
      final int numToSend = sender.getNumBlocksToMove();
      final int numToReceive = receiver.getNumBlocksToMove();
      final int numToMove = Math.min(sender.getNumBlocksToMove(), receiver.getNumBlocksToMove());
      
      planBuilder.addTransferStep(namespace,
          new ILPTransferStepImpl(Integer.toString(sender.getEvalId()), Integer.toString(receiver.getEvalId()),
              new DataInfoImpl(numToMove)));
      
      if (numToSend == numToReceive) {
        continue;
      } else if (numToMove == numToSend) {
        receiver.setNumBlocksToMove(numToReceive - numToMove);
        receiverPriorityQueue.add(receiver);
      } else {
        sender.setNumBlocksToMove(numToSend - numToMove);
        senderPriorityQueue.add(sender);
      }
    }
  }
  
  private static final Comparator<BlockDelta> NUM_BLOCKS_TO_MOVE_COMPARATOR =
      (o1, o2) -> {
        return o2.getNumBlocksToMove() - o1.getNumBlocksToMove();
      };
}
