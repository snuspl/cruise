/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.cay.dolphin.bsp.core.optimizer;

import edu.snu.cay.dolphin.bsp.core.CtrlTaskContextIdFetcher;
import edu.snu.cay.services.em.optimizer.api.DataInfo;
import edu.snu.cay.services.em.optimizer.api.EvaluatorParameters;
import edu.snu.cay.services.em.optimizer.api.Optimizer;
import edu.snu.cay.services.em.optimizer.impl.DataInfoImpl;
import edu.snu.cay.services.em.plan.api.Plan;
import edu.snu.cay.services.em.plan.api.TransferStep;
import edu.snu.cay.services.em.plan.impl.PlanImpl;
import edu.snu.cay.services.em.plan.impl.TransferStepImpl;
import edu.snu.cay.utils.Tuple3;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.util.Optional;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.cay.dolphin.bsp.core.optimizer.OptimizationOrchestrator.*;

/**
 * Optimizer class based on the optimization formula used by {@link ILPSolverOptimizer},
 * solved by hand instead of using an ILP solver library, with an additional constraint applied.
 *
 * Similar to {@link ILPSolverOptimizer}, this class assumes that
 * there is a single controller task and several compute tasks.
 * Computation costs per block for new evaluators are set to the
 * average value of computation costs per block for all existing evaluators.
 *
 * This optimizer also assumes that there are no cases where
 * addition and deletion occurs at the same time. In other words,
 * stragglers can get deleted, but are not replaced with a new evaluator
 * in the same plan (this assumption is not explicit; rather, it is a result
 * of how this optimizer handles variables)
 *
 * <p>
 *   Optimization problem:<br>
 *     Find the data block distribution <i>(w1, w2, ... wn)</i> that minimizes <i>M = max(c1w1, c2w2, ..., cnwn)</i>,
 *     under the following conditions. <i>M</i> corresponds to the computation cost of an iteration.
 *     <ul>
 *       <li><i>wi</i></li>: positive; number of data blocks for task <i>i</i>
 *       <li><i>w1 + w2 + ... + wn = W</i> (positive integer constant)</li>
 *       <li><i>ci</i></li>: positive real constants;
 *          time required to process a data block for task <i>i</i> (computation cost per block)
 *       <li><i>n</i></li>: positive integer constant; total number of tasks
 *     </ul>
 * </p>
 *
 * <p>
 *   Solution:<br>
 *     <i>M</i> is minimized if and only if <i>c1w1 = c2w2 = ... = cnwn</i>, which leads us to our solution:<br>
 *       <ul><li><i>wi = W / ci / (1 / c1 + 1 / c2 + ... + 1 / cn)</i></li></ul>
 *     Since in the real world the number of data blocks is a whole number,
 *     we round the <i>wi</i> values to the nearest integers.<br>
 *     In fact, the total cost (comm cost + comp cost) can now be calculated as<br>
 *       <ul><li><i>cost = W / (1 / c1 + 1 / c2 + ... 1 / cn) + k * n</i></li></ul>
 *     Using this formula, we can 1) find the number of tasks <i>n</i> that can produce the least cost, and
 *     2) find the data block distribution that gives us that least cost.
 * </p>
 */
public final class ILPQuickOptimizer implements Optimizer {

  private static final Logger LOG = Logger.getLogger(ILPQuickOptimizer.class.getName());
  private static final String NEW_COMPUTE_TASK_ID_PREFIX = "newComputeTask-";

  /**
   * Comparator to sort {@link OptimizedEvaluator}s put in a priority queue,
   * by their inverse computation costs per block.
   * {@link OptimizedEvaluator}s with greater inverse computation costs per block (i.e. quicker evaluators)
   * are put at the front (descending order).
   */
  private static final Comparator<OptimizedEvaluator> COMP_COST_PER_BLOCK_INV_COMPARATOR =
      new Comparator<OptimizedEvaluator>() {
        @Override
        public int compare(final OptimizedEvaluator o1, final OptimizedEvaluator o2) {
          if (o1.compCostPerBlockInv < o2.compCostPerBlockInv) {
            return 1;
          } else if (o1.compCostPerBlockInv > o2.compCostPerBlockInv) {
            return -1;
          } else {
            return 0;
          }
        }
      };

  /**
   * Comparator to sort {@link OptimizedEvaluator}s put in a priority queue, by their number of blocks to move.
   * {@link OptimizedEvaluator}s with more blocks to move are put at the front (descending order).
   */
  private static final Comparator<OptimizedEvaluator> NUM_BLOCKS_TO_MOVE_COMPARATOR =
      new Comparator<OptimizedEvaluator>() {
        @Override
        public int compare(final OptimizedEvaluator o1, final OptimizedEvaluator o2) {
          final int numBlocksToMove1 = Math.abs(o1.getNumBlocks() - o1.numOptimalBlocks);
          final int numBlocksToMove2 = Math.abs(o2.getNumBlocks() - o2.numOptimalBlocks);
          return numBlocksToMove2 - numBlocksToMove1;
        }
      };


  /**
   * Fetcher object of the ID of the Context that goes under Controller Task.
   */
  private final CtrlTaskContextIdFetcher ctrlTaskContextIdFetcher;

  /**
   * Injectable constructor for this class.
   * Use {@link CtrlTaskContextIdFetcher} to identify the controller task.
   */
  @Inject
  private ILPQuickOptimizer(final CtrlTaskContextIdFetcher ctrlTaskContextIdFetcher) {
    this.ctrlTaskContextIdFetcher = ctrlTaskContextIdFetcher;
  }

  @Override
  public Plan optimize(final Map<String, List<EvaluatorParameters>> evalParamsMap, final int numAvailableEvals) {
    final Optional<String> ctrlTaskContextId = ctrlTaskContextIdFetcher.getCtrlTaskContextId();
    if (!ctrlTaskContextId.isPresent()) {
      LOG.log(Level.WARNING, "Controller task is unidentifiable at the moment. Returning empty plan.");
      return PlanImpl.newBuilder().build();
    }

    final List<EvaluatorParameters> activeEvaluators = evalParamsMap.get(NAMESPACE_DOLPHIN_BSP);

    // Step 1: Process metrics into meaningful values
    final Optional<Cost> cost = CostCalculator.calculate(
        activeEvaluators,
        ctrlTaskContextId.get());
    if (!cost.isPresent()) {
      LOG.log(Level.WARNING, "No controller task present at the moment. Returning empty plan.");
      return PlanImpl.newBuilder().build();
    }

    // Step 2: Create OptimizedEvaluators that contain compute cost per block info.
    // Evaluators with good performance are put at the front of the queue, while
    // the bad ones are put at the back.
    // Later, the worst evaluators are selected to be removed in case deletion is needed.
    final Tuple3<PriorityQueue<OptimizedEvaluator>, Double, Integer> optimizedEvalTuple =
        getOptimizedEvaluators(cost.get());
    final PriorityQueue<OptimizedEvaluator> optimizedEvalQueue = optimizedEvalTuple.getFirst();
    final double compCostSum = optimizedEvalTuple.getSecond();
    final int numTotalBlocks = optimizedEvalTuple.getThird();

    // Step 3: Compute the optimal number of evaluators ('n') to use.
    final int numCurrEvals = cost.get().getComputeTaskCosts().size();
    final double commCostWeight = cost.get().getCommunicationCost() / numCurrEvals; // corresponds to 'k'
    final double expectedCompCostPerBlockInv = numCurrEvals / compCostSum; // expected compCostPerBlockInv for new evals
    // assumption: compCostPerBlock for a new eval == average of compCostPerBlock for existing evals

    final Pair<Integer, Double> numOptimalEvalsPair = getNumOptimalEvals(
        optimizedEvalQueue, numAvailableEvals, commCostWeight, expectedCompCostPerBlockInv, numTotalBlocks);
    final int numOptimalEvals = numOptimalEvalsPair.getFirst();
    final double compCostPerBlockInvSum = numOptimalEvalsPair.getSecond();

    // Step 4: Compute the optimal number of data blocks for each evaluator.
    final PlanImpl.Builder planBuilder = PlanImpl.newBuilder();
    final Set<OptimizedEvaluator> evalSet = setNumOptimalBlocksForEvaluators(
        optimizedEvalQueue, expectedCompCostPerBlockInv, compCostPerBlockInvSum, numTotalBlocks, numOptimalEvals,
        planBuilder);

    // Step 5: Generate transfer steps according to the optimal plan.
    generateTransferSteps(NAMESPACE_DOLPHIN_BSP, evalSet, planBuilder);

    return planBuilder.build();
  }

  /**
   * Helper method that processes the given {@link Cost} and generates {@link OptimizedEvaluator}s that are sorted
   * by cost per block in a {@link PriorityQueue}.
   *
   * @return a tuple of a {@link PriorityQueue} of {@link OptimizedEvaluator}s,
   *   the sum of computation costs of all evaluators, and the total number of data blocks
   */
  private static Tuple3<PriorityQueue<OptimizedEvaluator>, Double, Integer> getOptimizedEvaluators(final Cost cost) {

    final PriorityQueue<OptimizedEvaluator> optimizedEvaluatorQueue =
        new PriorityQueue<>(cost.getComputeTaskCosts().size(), COMP_COST_PER_BLOCK_INV_COMPARATOR);
    double compCostPerBlockSum = 0;
    int numTotalBlocks = 0;

    for (final Cost.ComputeTaskCost computeTaskCost : cost.getComputeTaskCosts()) {
      final int numBlocks = computeTaskCost.getDataInfo().getNumBlocks();

      final double compCostPerBlock = computeTaskCost.getComputeCost() / numBlocks;
      final double compCostInv = 1 / compCostPerBlock;
      compCostPerBlockSum += compCostPerBlock;
      numTotalBlocks += numBlocks;

      optimizedEvaluatorQueue.add(new OptimizedEvaluator(
          computeTaskCost.getId(), computeTaskCost.getDataInfo(), compCostInv));
    }

    return new Tuple3<>(optimizedEvaluatorQueue, compCostPerBlockSum, numTotalBlocks);
  }

  /**
   * Helper method that computes the number of evaluators that produces the least cost.
   * Iterate over {@code 1} to {@code availableEvaluators - 1} and select the value with the least cost.
   *
   * @return the optimal number of evaluators to use,
   * and the sum of {@code compCostPerBlockInv}s for that optimal number
   */
  private static Pair<Integer, Double> getNumOptimalEvals(
      final PriorityQueue<OptimizedEvaluator> optimizedEvaluatorQueue,
      final int availableEvaluators,
      final double commCostWeight,
      final double expectedCompCostPerBlockInv,
      final int numTotalBlocks) {

    final Iterator<OptimizedEvaluator> iter = optimizedEvaluatorQueue.iterator();
    double compCostPerBlockInvSum = 0;
    double minCost = Double.MAX_VALUE;
    int numOptimalEvals = 0;

    // new evaluators are not considered until all original evaluators are 'used'
    // this is main part that leads to the assumption, 'no addition and deletion at the same time'
    for (int numEvals = 1; numEvals < availableEvaluators; ++numEvals) {
      final double currCompCostPerBlockInvSum;
      if (iter.hasNext()) {
        final OptimizedEvaluator eval = iter.next();
        currCompCostPerBlockInvSum = compCostPerBlockInvSum + eval.compCostPerBlockInv;
      } else {
        currCompCostPerBlockInvSum = compCostPerBlockInvSum + expectedCompCostPerBlockInv;
      }

      final double compCost = numTotalBlocks / currCompCostPerBlockInvSum;
      final double commCost = commCostWeight * numEvals;
      final double cost = compCost + commCost;
      if (cost < minCost) {
        minCost = cost;
        numOptimalEvals = numEvals;
        compCostPerBlockInvSum = currCompCostPerBlockInvSum;
      } else {
        // local minimum == global minimum, for the given formula
        break;
      }
    }

    return new Pair<>(numOptimalEvals, compCostPerBlockInvSum);
  }

  /**
   * Helper method that computes and sets the number of blocks for each evaluator
   * that make the computation cost ('M') become minimal.
   *
   * @return the set of optimized evaluators that produce the least cost, including new evaluators that are empty
   */
  private static Set<OptimizedEvaluator> setNumOptimalBlocksForEvaluators(
      final PriorityQueue<OptimizedEvaluator> optimizedEvaluatorQueue,
      final double expectedCompCostPerBlockInv,
      final double compCostPerBlockInvSum,
      final int numTotalBlocks,
      final int numOptimalEvals,
      final PlanImpl.Builder planBuilder) {

    final Set<OptimizedEvaluator> retSet = new HashSet<>(numOptimalEvals);
    final int numNewEvals = numOptimalEvals - optimizedEvaluatorQueue.size();
    int remainingBlocks = numTotalBlocks;

    for (int newEvalIndex = 0; newEvalIndex < numNewEvals; ++newEvalIndex) {
      final String newEvalId = NEW_COMPUTE_TASK_ID_PREFIX + newEvalIndex;
      final int newNumBlocks = (int)Math.round(numTotalBlocks * expectedCompCostPerBlockInv / compCostPerBlockInvSum);
      retSet.add(new OptimizedEvaluator(newEvalId, new DataInfoImpl(),
          expectedCompCostPerBlockInv, newNumBlocks));
      planBuilder.addEvaluatorToAdd(NAMESPACE_DOLPHIN_BSP, newEvalId);
      remainingBlocks -= newNumBlocks;
    }

    for (final OptimizedEvaluator eval : optimizedEvaluatorQueue) {
      if (retSet.size() >= numOptimalEvals) {
        // the optimal number of evaluators has been reached; delete the remaining ones
        planBuilder.addEvaluatorToDelete(NAMESPACE_DOLPHIN_BSP, eval.id);
        eval.numOptimalBlocks = 0;

      } else if (retSet.size() == numOptimalEvals - 1) {
        // this is the last evaluator to be included
        // we need this if-case to prevent +1/-1 errors from Math.round()
        eval.numOptimalBlocks = remainingBlocks;

      } else {
        final int newNumBlocks = (int) Math.round(numTotalBlocks * eval.compCostPerBlockInv / compCostPerBlockInvSum);
        eval.numOptimalBlocks = newNumBlocks;
        remainingBlocks -= newNumBlocks;
      }

      retSet.add(eval);
    }

    return retSet;
  }

  /**
   * Helper method that generates {@link TransferStep}s using the given {@code optimizedEvalSet}
   * and adds them to the given {@link PlanImpl.Builder}.
   * Very similar to {@link ILPSolverOptimizer}, except that
   * code was modified to match the {@link OptimizedEvaluator} methods and types.
   */
  private static void generateTransferSteps(final String namespace,
                                            final Set<OptimizedEvaluator> optimizedEvalSet,
                                            final PlanImpl.Builder builder) {
    final PriorityQueue<OptimizedEvaluator> senderPriorityQueue =
        new PriorityQueue<>(optimizedEvalSet.size(), NUM_BLOCKS_TO_MOVE_COMPARATOR);
    final PriorityQueue<OptimizedEvaluator> receiverPriorityQueue =
        new PriorityQueue<>(optimizedEvalSet.size(), NUM_BLOCKS_TO_MOVE_COMPARATOR);

    for (final OptimizedEvaluator eval : optimizedEvalSet) {
      if (eval.getNumBlocks() > eval.numOptimalBlocks) {
        senderPriorityQueue.add(eval);
      } else if (eval.getNumBlocks() < eval.numOptimalBlocks) {
        receiverPriorityQueue.add(eval);
      }
    }

    // greedy search for generating transfer steps
    while (senderPriorityQueue.size() > 0) {
      // pick the compute task that has the biggest amount of data blocks to send to be the sender
      final OptimizedEvaluator sender = senderPriorityQueue.poll();
      while (sender.getNumBlocks() > sender.numOptimalBlocks) {
        // pick the compute task that has the biggest amount of data blocks to receive to be the receiver
        final OptimizedEvaluator receiver = receiverPriorityQueue.poll();
        builder.addTransferStep(namespace, generateTransferStep(sender, receiver));
        if (receiver.getNumBlocks() < receiver.numOptimalBlocks) {
          receiverPriorityQueue.add(receiver);
          break;
        }
      }
    }
  }

  /**
   * Helper method that creates {@link TransferStep} that best fits the given optimization plan,
   * for {@code sender} and {@code receiver}.
   */
  private static TransferStep generateTransferStep(final OptimizedEvaluator sender,
                                                   final OptimizedEvaluator receiver) {
    final int numToSend = sender.getNumBlocks() - sender.numOptimalBlocks;
    final int numToReceive = receiver.numOptimalBlocks - receiver.getNumBlocks();
    final int numToMove = Math.min(numToSend, numToReceive);

    return new TransferStepImpl(sender.id, receiver.id, new DataInfoImpl(numToMove));
  }

  private static final class OptimizedEvaluator {
    private final String id;
    private final DataInfo dataInfo;
    private final double compCostPerBlockInv;
    private int numOptimalBlocks;

    private OptimizedEvaluator(final String id, final DataInfo dataInfo, final double compCostPerBlockInv) {
      this(id, dataInfo, compCostPerBlockInv, 0);
    }

    private OptimizedEvaluator(final String id, final DataInfo dataInfo, final double compCostPerBlockInv,
                               final int numOptimalBlocks) {
      this.id = id;
      this.compCostPerBlockInv = compCostPerBlockInv;
      this.numOptimalBlocks = numOptimalBlocks;
      this.dataInfo = dataInfo;
    }

    private int getNumBlocks() {
      return dataInfo.getNumBlocks();
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("OptimizedEvaluator[id=")
          .append(id)
          .append(", numBlocks=")
          .append(getNumBlocks())
          .append(", compCostPerBlockInv=")
          .append(compCostPerBlockInv)
          .append(", numOptimalBlocks=")
          .append(numOptimalBlocks)
          .append("]");
      return sb.toString();
    }
  }
}
