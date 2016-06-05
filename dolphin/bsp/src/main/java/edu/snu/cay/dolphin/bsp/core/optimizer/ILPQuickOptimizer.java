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
 * there is a single controller task and several compute tasks,
 * and that data is homogeneous (all data types are treated the same).
 * Computation unit costs for new evaluators are set to the
 * average value of computation unit costs for all existing evaluators.
 *
 * This optimizer also assumes that there are no cases where
 * addition and deletion occurs at the same time. In other words,
 * stragglers can get deleted, but are not replaced with a new evaluator
 * in the same plan (this assumption is not explicit; rather, it is a result
 * of how this optimizer handles variables)
 *
 * <p>
 *   Optimization problem:<br>
 *     Find the workload distribution <i>(w1, w2, ... wn)</i> that minimizes <i>M = max(c1w1, c2w2, ..., cnwn)</i>,
 *     under the following conditions. <i>M</i> corresponds to the computation cost of an iteration.
 *     <ul>
 *       <li><i>wi</i></li>: positive; number of data units for task <i>i</i> (workload)
 *       <li><i>w1 + w2 + ... + wn = W</i> (positive integer constant)</li>
 *       <li><i>ci</i></li>: positive real constants;
 *          time required to process a single data unit for task <i>i</i> (computation unit cost)
 *       <li><i>n</i></li>: positive integer constant; total number of tasks
 *     </ul>
 * </p>
 *
 * <p>
 *   Solution:<br>
 *     <i>M</i> is minimized if and only if <i>c1w1 = c2w2 = ... = cnwn</i>, which leads us to our solution:<br>
 *       <ul><li><i>wi = W / ci / (1 / c1 + 1 / c2 + ... + 1 / cn)</i></li></ul>
 *     Since in the real world the number of data units is a whole number,
 *     we round the <i>wi</i> values to the nearest integers.<br>
 *     In fact, the total cost (comm cost + comp cost) can now be calculated as<br>
 *       <ul><li><i>cost = W / (1 / c1 + 1 / c2 + ... 1 / cn) + k * n</i></li></ul>
 *     Using this formula, we can 1) find the number of tasks <i>n</i> that can produce the least cost, and
 *     2) find the workload distribution that gives us that least cost.
 * </p>
 */
public final class ILPQuickOptimizer implements Optimizer {

  private static final Logger LOG = Logger.getLogger(ILPQuickOptimizer.class.getName());
  private static final String NEW_COMPUTE_TASK_ID_PREFIX = "newComputeTask-";

  /**
   * Comparator to sort {@link OptimizedEvaluator}s put in a priority queue, by their inverse computation unit costs.
   * {@link OptimizedEvaluator}s with greater inverse computation unit costs (i.e. quicker evaluators)
   * are put at the front (descending order).
   */
  private static final Comparator<OptimizedEvaluator> COMP_UNIT_COST_INV_COMPARATOR =
      new Comparator<OptimizedEvaluator>() {
        @Override
        public int compare(final OptimizedEvaluator o1, final OptimizedEvaluator o2) {
          if (o1.compUnitCostInv < o2.compUnitCostInv) {
            return 1;
          } else if (o1.compUnitCostInv > o2.compUnitCostInv) {
            return -1;
          } else {
            return 0;
          }
        }
      };

  /**
   * Comparator to sort {@link OptimizedEvaluator}s put in a priority queue, by their number of units to move.
   * {@link OptimizedEvaluator}s with more units to move are put at the front (descending order).
   */
  private static final Comparator<OptimizedEvaluator> NUM_UNITS_TO_MOVE_COMPARATOR =
      new Comparator<OptimizedEvaluator>() {
        @Override
        public int compare(final OptimizedEvaluator o1, final OptimizedEvaluator o2) {
          final int numUnitsToMove1 = Math.abs(o1.getNumUnits() - o1.numOptimalUnits);
          final int numUnitsToMove2 = Math.abs(o2.getNumUnits() - o2.numOptimalUnits);
          return numUnitsToMove2 - numUnitsToMove1;
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
  public Plan optimize(final Map<String, List<EvaluatorParameters>> evalParamsMap, final int availableEvaluators) {
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

    // Step 2: Create OptimizedEvaluators that contain compute unit cost info.
    // Evaluators with good performance are put at the front of the queue, while
    // the bad ones are put at the back.
    // Later, the worst evaluators are selected to be removed in case deletion is needed.
    final Tuple3<PriorityQueue<OptimizedEvaluator>, Double, Integer> optimizedEvalTuple =
        getOptimizedEvaluators(cost.get());
    final PriorityQueue<OptimizedEvaluator> optimizedEvalQueue = optimizedEvalTuple.getFirst();
    final double compUnitCostSum = optimizedEvalTuple.getSecond();
    final int numUnitsTotal = optimizedEvalTuple.getThird();

    // Step 3: Compute the optimal number of evaluators ('n') to use.
    final int numCurrEvals = cost.get().getComputeTaskCosts().size();
    final double commCostWeight = cost.get().getCommunicationCost() / numCurrEvals; // corresponds to 'k'
    final double expectedCompUnitCostInv = numCurrEvals / compUnitCostSum; // expected compUnitCostInv for new evals
    // assumption: compUnitCost for a new eval == average of compUnitCost for existing evals

    final Pair<Integer, Double> numOptimalEvalsPair = getNumOptimalEvals(
        optimizedEvalQueue, availableEvaluators, commCostWeight, expectedCompUnitCostInv, numUnitsTotal);
    final int numOptimalEvals = numOptimalEvalsPair.getFirst();
    final double compUnitCostInvSum = numOptimalEvalsPair.getSecond();

    // Step 4: Compute the optimal number of data units for each evaluator.
    final PlanImpl.Builder planBuilder = PlanImpl.newBuilder();
    final Set<OptimizedEvaluator> evalSet = setNumOptimalUnitsForEvaluators(
        optimizedEvalQueue, expectedCompUnitCostInv, compUnitCostInvSum, numUnitsTotal, numOptimalEvals, planBuilder);

    // Step 5: Generate transfer steps according to the optimal plan.
    generateTransferSteps(NAMESPACE_DOLPHIN_BSP, evalSet, planBuilder);

    return planBuilder.build();
  }

  /**
   * Helper method that processes the given {@link Cost} and generates {@link OptimizedEvaluator}s that are sorted
   * by unit cost in a {@link PriorityQueue}.
   *
   * @return a tuple of a {@link PriorityQueue} of {@link OptimizedEvaluator}s,
   *   the sum of computation unit costs of all evaluators, and the total number of data units
   */
  private static Tuple3<PriorityQueue<OptimizedEvaluator>, Double, Integer> getOptimizedEvaluators(final Cost cost) {

    final PriorityQueue<OptimizedEvaluator> optimizedEvaluatorQueue =
        new PriorityQueue<>(cost.getComputeTaskCosts().size(), COMP_UNIT_COST_INV_COMPARATOR);
    double compUnitCostSum = 0;
    int numUnitsTotal = 0;

    for (final Cost.ComputeTaskCost computeTaskCost : cost.getComputeTaskCosts()) {
      final int numUnits = computeTaskCost.getDataInfo().getNumUnits();

      final double compUnitCost = computeTaskCost.getComputeCost() / numUnits;
      final double compUnitCostInv = 1 / compUnitCost;
      compUnitCostSum += compUnitCost;
      numUnitsTotal += numUnits;

      optimizedEvaluatorQueue.add(new OptimizedEvaluator(
          computeTaskCost.getId(), computeTaskCost.getDataInfo(), compUnitCostInv));
    }

    return new Tuple3<>(optimizedEvaluatorQueue, compUnitCostSum, numUnitsTotal);
  }

  /**
   * Helper method that computes the number of evaluators that produces the least cost.
   * Iterate over {@code 1} to {@code availableEvaluators - 1} and select the value with the least cost.
   *
   * @return the optimal number of evaluators to use, and the sum of {@code compUnitCostInv}s for that optimal number
   */
  private static Pair<Integer, Double> getNumOptimalEvals(
      final PriorityQueue<OptimizedEvaluator> optimizedEvaluatorQueue,
      final int availableEvaluators,
      final double commCostWeight,
      final double expectedCompUnitCostInv,
      final int totalWorkload) {

    final Iterator<OptimizedEvaluator> iter = optimizedEvaluatorQueue.iterator();
    double compUnitCostInvSum = 0;
    double minCost = Double.MAX_VALUE;
    int numOptimalEvals = 0;

    // new evaluators are not considered until all original evaluators are 'used'
    // this is main part that leads to the assumption, 'no addition and deletion at the same time'
    for (int numEvals = 1; numEvals < availableEvaluators; ++numEvals) {
      final double currCompUnitCostInvSum;
      if (iter.hasNext()) {
        final OptimizedEvaluator eval = iter.next();
        currCompUnitCostInvSum = compUnitCostInvSum + eval.compUnitCostInv;
      } else {
        currCompUnitCostInvSum = compUnitCostInvSum + expectedCompUnitCostInv;
      }

      final double compCost = totalWorkload / currCompUnitCostInvSum;
      final double commCost = commCostWeight * numEvals;
      final double cost = compCost + commCost;
      if (cost < minCost) {
        minCost = cost;
        numOptimalEvals = numEvals;
        compUnitCostInvSum = currCompUnitCostInvSum;
      } else {
        // local minimum == global minimum, for the given formula
        break;
      }
    }

    return new Pair<>(numOptimalEvals, compUnitCostInvSum);
  }

  /**
   * Helper method that computes and sets the number of units for each evaluator
   * that make the computation cost ('M') become minimal.
   *
   * @return the set of optimized evaluators that produce the least cost, including new evaluators that are empty
   */
  private static Set<OptimizedEvaluator> setNumOptimalUnitsForEvaluators(
      final PriorityQueue<OptimizedEvaluator> optimizedEvaluatorQueue,
      final double expectedCompUnitCostInv,
      final double compUnitCostInvSum,
      final int totalWorkload,
      final int numOptimalEvals,
      final PlanImpl.Builder planBuilder) {

    final Set<OptimizedEvaluator> retSet = new HashSet<>(numOptimalEvals);
    final int numNewEvals = numOptimalEvals - optimizedEvaluatorQueue.size();
    int remainingWorkload = totalWorkload;

    for (int newEvalIndex = 0; newEvalIndex < numNewEvals; ++newEvalIndex) {
      final String newEvalId = NEW_COMPUTE_TASK_ID_PREFIX + newEvalIndex;
      final int newWorkload = (int)Math.round(totalWorkload * expectedCompUnitCostInv / compUnitCostInvSum);
      retSet.add(new OptimizedEvaluator(newEvalId, new DataInfoImpl(),
          expectedCompUnitCostInv, newWorkload));
      planBuilder.addEvaluatorToAdd(NAMESPACE_DOLPHIN_BSP, newEvalId);
      remainingWorkload -= newWorkload;
    }

    for (final OptimizedEvaluator eval : optimizedEvaluatorQueue) {
      if (retSet.size() >= numOptimalEvals) {
        // the optimal number of evaluators has been reached; delete the remaining ones
        planBuilder.addEvaluatorToDelete(NAMESPACE_DOLPHIN_BSP, eval.id);
        eval.numOptimalUnits = 0;

      } else if (retSet.size() == numOptimalEvals - 1) {
        // this is the last evaluator to be included
        // we need this if-case to prevent +1/-1 errors from Math.round()
        eval.numOptimalUnits = remainingWorkload;

      } else {
        final int newWorkload = (int) Math.round(totalWorkload * eval.compUnitCostInv / compUnitCostInvSum);
        eval.numOptimalUnits = newWorkload;
        remainingWorkload -= newWorkload;
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
        new PriorityQueue<>(optimizedEvalSet.size(), NUM_UNITS_TO_MOVE_COMPARATOR);
    final PriorityQueue<OptimizedEvaluator> receiverPriorityQueue =
        new PriorityQueue<>(optimizedEvalSet.size(), NUM_UNITS_TO_MOVE_COMPARATOR);

    for (final OptimizedEvaluator eval : optimizedEvalSet) {
      if (eval.getNumUnits() > eval.numOptimalUnits) {
        senderPriorityQueue.add(eval);
      } else if (eval.getNumUnits() < eval.numOptimalUnits) {
        receiverPriorityQueue.add(eval);
      }
    }

    // greedy search for generating transfer steps
    while (senderPriorityQueue.size() > 0) {
      // pick the compute task that has the biggest amount of data units to send to be the sender
      final OptimizedEvaluator sender = senderPriorityQueue.poll();
      while (sender.getNumUnits() > sender.numOptimalUnits) {
        // pick the compute task that has the biggest amount of data units to receive to be the receiver
        final OptimizedEvaluator receiver = receiverPriorityQueue.poll();
        builder.addTransferStep(namespace, generateTransferStep(sender, receiver));
        if (receiver.getNumUnits() < receiver.numOptimalUnits) {
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
    final int numToSend = sender.getNumUnits() - sender.numOptimalUnits;
    final int numToReceive = receiver.numOptimalUnits - receiver.getNumUnits();
    final int numToMove = Math.min(numToSend, numToReceive);

    return new TransferStepImpl(sender.id, receiver.id, new DataInfoImpl(numToMove));
  }

  private static final class OptimizedEvaluator {
    private final String id;
    private final DataInfo dataInfo;
    private final double compUnitCostInv;
    private int numOptimalUnits;

    private OptimizedEvaluator(final String id, final DataInfo dataInfo, final double compUnitCostInv) {
      this(id, dataInfo, compUnitCostInv, 0);
    }

    private OptimizedEvaluator(final String id, final DataInfo dataInfo, final double compUnitCostInv,
                               final int numOptimalUnits) {
      this.id = id;
      this.compUnitCostInv = compUnitCostInv;
      this.numOptimalUnits = numOptimalUnits;
      this.dataInfo = dataInfo;
    }

    private int getNumUnits() {
      return dataInfo.getNumUnits();
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("OptimizedEvaluator[id=")
          .append(id)
          .append(", numUnits=")
          .append(getNumUnits())
          .append(", compUnitCostInv=")
          .append(compUnitCostInv)
          .append(", numOptimalUnits=")
          .append(numOptimalUnits)
          .append("]");
      return sb.toString();
    }
  }
}
