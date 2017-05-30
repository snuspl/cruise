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
import edu.snu.cay.dolphin.async.plan.impl.TransferStepImpl;

import javax.inject.Inject;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * Generate ILP plan with the result solved by ILP solver. Generated ILP plan is saved in {@link ILPPlanDescriptor}.
 */
public final class ILPPlanGenerator {

  private static final int NO_RECEIVER_ERROR = -1;

  /**
   * Average block size of worker and server should be computed.
   */
  private final Map<String, Integer> blockSize = new HashMap<>(2);

  @Inject
  private ILPPlanGenerator() {
    // blockSize.put(Constants.NAMESPACE_WORKER, blockStore.get(blockPartitioner.getBlockId(0)).getNumPairs());
    blockSize.put(Constants.NAMESPACE_WORKER, 10);
    blockSize.put(Constants.NAMESPACE_SERVER, 10);
  }

  /**
   * Generate {@link ILPPlanDescriptor} with solution solved by ILP solver.
   * @param oldRole role of each evaluator before optimization is applied.
   * @param oldD number of data in each evaluator before optimization is applied.
   * @param oldM number of models in each evaluator before optimization is applied.
   * @param newRole role of each evaluator after optimization is applied.
   * @param newD number of data in each evaluator after optimization is applied.
   * @param newM number of models in each evaluator after optimization is applied.
   * @return block transferring plan for optimization.
   */
  public ILPPlanDescriptor generatePlanDescriptor(final int[] oldRole, final int[] oldD, final int[] oldM,
                                                   final int[] newRole, final int[] newD, final int[] newM) {
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

  /**
   * Generate block transferring plan to apply optimization.
   * 1. Distinguish sender and receiver by comparing {@param oldState} and {@param newState}.
   * 2. Sort {@code senderPriorityQueue} and {@code receiverPriorityQueue} in descending order with the number of
   *    blocks to transfer.
   * 3. Add transferring plan until both queue is not empty.
   * 4. If sender queue is not empty while receiver queue is empty, send all the remaining blocks to
   *    {@code definiteReceiver}.
   * @param namespace indicates whether this plan is for server or worker.
   * @param oldState
   * @param newState
   * @param planBuilder
   */
  private void generateTransferPlans(final String namespace, final int[] oldState, final int[] newState,
                                     final ILPPlanDescriptor.Builder planBuilder) {
    final int numTotalEval = oldState.length;
    final PriorityQueue<BlockDelta> senderPriorityQueue =
        new PriorityQueue<>(numTotalEval, NUM_BLOCKS_TO_MOVE_COMPARATOR);
    final PriorityQueue<BlockDelta> receiverPriorityQueue =
        new PriorityQueue<>(numTotalEval, NUM_BLOCKS_TO_MOVE_COMPARATOR);
    for (int i = 0; i < numTotalEval; i++) {
      final int numBlocksToMove = oldState[i] / blockSize.get(namespace) - newState[i] / blockSize.get(namespace);
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
          new TransferStepImpl(Integer.toString(sender.getEvalId()), Integer.toString(receiver.getEvalId()),
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

    if (!senderPriorityQueue.isEmpty()) {
      int definiteReceiver = NO_RECEIVER_ERROR;
      for (int i = 0; i < numTotalEval; i++) {
        if (newState[i] > 0) {
          definiteReceiver = i;
          break;
        }
      }
      if (definiteReceiver == NO_RECEIVER_ERROR) {
        throw new RuntimeException("There are only " + namespace + "components.");
      }
      while (!senderPriorityQueue.isEmpty()) {
        final BlockDelta sender = senderPriorityQueue.poll();
        planBuilder.addTransferStep(namespace,
            new TransferStepImpl(Integer.toString(sender.getEvalId()), Integer.toString(definiteReceiver),
                new DataInfoImpl(sender.getNumBlocksToMove())));
      }
    }
  }
  
  private static final Comparator<BlockDelta> NUM_BLOCKS_TO_MOVE_COMPARATOR =
      (o1, o2) -> o2.getNumBlocksToMove() - o1.getNumBlocksToMove();
}
