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
package edu.snu.cay.dolphin.core.optimizer;

import edu.snu.cay.services.em.optimizer.api.DataInfo;
import edu.snu.cay.services.em.optimizer.api.EvaluatorParameters;
import edu.snu.cay.services.em.optimizer.api.Optimizer;
import edu.snu.cay.services.em.optimizer.impl.DataInfoImpl;
import edu.snu.cay.services.em.plan.api.Plan;
import edu.snu.cay.services.em.plan.api.TransferStep;
import edu.snu.cay.services.em.plan.impl.PlanImpl;
import edu.snu.cay.services.em.plan.impl.TransferStepImpl;
import org.ojalgo.optimisation.Expression;
import org.ojalgo.optimisation.ExpressionsBasedModel;
import org.ojalgo.optimisation.Optimisation;
import org.ojalgo.optimisation.Variable;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Optimizer class based on ILP (integer linear programming)
 * Assumes that the active evaluators consists of a single controller task and compute tasks.
 */
public final class ILPSolverOptimizer implements Optimizer {

  private static final String NEW_EVALUATOR_ID_PREFIX = "newEvaluator-";
  private static final String NEW_TEMP_EVALUATOR_ID_PREFIX = "newTempEvaluator-";
  private final AtomicInteger newEvaluatorSequence = new AtomicInteger(0);
  private final DataUnitsToMoveComparator ascendingComparator;
  private final DataUnitsToMoveComparator descendingComparator;

  @Inject
  private ILPSolverOptimizer() {
    this.ascendingComparator = new DataUnitsToMoveComparator();
    this.descendingComparator = new DataUnitsToMoveComparator(DataUnitsToMoveComparator.Order.DESCENDING);
  }

  @Override
  public Plan optimize(final Collection<EvaluatorParameters> activeEvaluators, final int availableEvaluators) {

    final Cost cost = CostCalculator.newInstance(activeEvaluators).calculate();
    final List<OptimizedEvaluator> optimizedEvaluators =
        initOptimizedEvaluators(cost, availableEvaluators - activeEvaluators.size());

    // create ILP model for optimization
    // C_cmp: expected compute cost
    // C_comm': communication cost
    // C_comm: expected communication cost
    // #_active' : the number of active compute tasks
    // #_active : the expected number of active compute tasks
    // objective function: C_cmp + C_comm s.t. C_comm = C_comm' / #_active' * #_active
    final ExpressionsBasedModel model = new ExpressionsBasedModel();
    final double commCostWeight = cost.getCommunicationCost() / cost.getComputeTaskCosts().size();

    final Variable cmpCostVar = Variable.make("computeCost").weight(1.0);
    model.addVariable(cmpCostVar);

    for (final OptimizedEvaluator evaluator : optimizedEvaluators) {
      model.addVariable(evaluator.getParticipateVariable().weight(commCostWeight));
      model.addVariable(evaluator.getRequestedDataVariable());
    }

    addExpressions(model, cmpCostVar, optimizedEvaluators, availableEvaluators - 1); // -1 for excluding the ctrl task

    final Optimisation.Result result = model.minimise();
    System.out.println(result);

    return generatePlan(optimizedEvaluators);
  }

  private static List<OptimizedEvaluator> initOptimizedEvaluators(final Cost cost,
                                                                  final int unusedEvaluators) {
    final List<OptimizedEvaluator> ret = new ArrayList<>(cost.getComputeTaskCosts().size() + unusedEvaluators);

    // create variables for active compute tasks
    for (final Cost.ComputeTaskCost cmpTaskCost : cost.getComputeTaskCosts()) {
      // add variables for data
      final Variable dataVar = getNewDataVariable(cmpTaskCost.getId());

      // create variable for the evaluator's participation to the model
      final Variable participateVar = getNewParticipateVariable(cmpTaskCost.getId());

      ret.add(new OptimizedEvaluator(
          cmpTaskCost.getId(), participateVar, dataVar, cmpTaskCost.getDataInfos(), cmpTaskCost.getComputeCost()));
    }

    // create variables for unused tasks
    for (int i = 0; i < unusedEvaluators; ++i) {
      final String id = NEW_TEMP_EVALUATOR_ID_PREFIX + "-" + i;
      final Variable participateVar = getNewParticipateVariable(id);
      final Variable dataVar = getNewDataVariable(id);
      ret.add(new OptimizedEvaluator(id, participateVar, dataVar));
    }

    return ret;
  }

  private static Variable getNewDataVariable(final String id) {
    return Variable.make("data-" + id).integer(true).lower(0);
  }

  private static Variable getNewParticipateVariable(final String id) {
    return Variable.makeBinary("participate-" + id);
  }

  private static void addExpressions(final ExpressionsBasedModel model,
                                     final Variable computeCostVariable,
                                     final List<OptimizedEvaluator> optimizedEvaluators,
                                     final int availableEvaluators) {
    // C_cmp'(i): compute cost for i-th evaluator.
    // C_cmp(i): expected compute cost for i-th evaluator.
    // d'(i): allocated data units for i-th evaluator.
    // d(i): requested data units for i-th evaluator.
    // p(i): indicator for the participation of evaluator i. if p(i) = 1, i-th evaluator participates next iteration.

    final int totalDataUnits = getSumDataUnits(optimizedEvaluators);
    final double predictedComputePerformance = predictComputePerformance(optimizedEvaluators);

    // [sum of data units] = sum(d(i))
    final Expression totalDataExpression = model.addExpression("totalDataExpression").level(totalDataUnits);
    // sum(p(i)) <= [# of available evaluators]
    final Expression totalEvaluatorExpression =
        model.addExpression("totalEvaluatorExpression").upper(availableEvaluators);
    System.out.println("availableEvaluators: " + availableEvaluators);

    for (final OptimizedEvaluator evaluator : optimizedEvaluators) {
      totalDataExpression.setLinearFactor(evaluator.getRequestedDataVariable(), 1);
      totalEvaluatorExpression.setLinearFactor(evaluator.getParticipateVariable(), 1);

      // d(i) <= p(i) * [sum of data units]
      final Expression dataExpression =
          model.addExpression("dataExpression-" + evaluator.getId()).lower(0);
      dataExpression.setLinearFactor(evaluator.getParticipateVariable(), totalDataUnits);
      dataExpression.setLinearFactor(evaluator.getRequestedDataVariable(), -1);

      // C_cmp = max(C_cmp(i)) i.e. for all i, C_cmp >= C_cmp(i) = C_cmp'(i) / d'(i) * d(i)
      final Expression computeCostExpression =
          model.addExpression("computeCostExpression-" + evaluator.getId()).upper(0);
      final int sumDataUnits = getSumDataUnits(evaluator.getAllocatedDataInfos());
      final double computePerformance;

      if (sumDataUnits > 0) {
        computePerformance = evaluator.getComputeCost() / sumDataUnits;
      } else {
        computePerformance = predictedComputePerformance;
      }

      computeCostExpression.setLinearFactor(evaluator.getRequestedDataVariable(), computePerformance);
      computeCostExpression.setLinearFactor(computeCostVariable, -1);
    }
  }

  private static int getSumDataUnits(final List<OptimizedEvaluator> optimizedEvaluators) {
    int sum = 0;
    for (final OptimizedEvaluator evaluator : optimizedEvaluators) {
      sum += getSumDataUnits(evaluator.getAllocatedDataInfos());
    }
    return sum;
  }

  private static int getSumDataUnits(final Collection<DataInfo> dataInfos) {
    int sum = 0;
    for (final DataInfo dataInfo : dataInfos) {
      sum += dataInfo.getNumUnits();
    }
    return sum;
  }

  /**
   * Generates an optimizer plan with a list of optimized evaluators.
   * The generated plan transfers data from the evaluator with the largest amount of data to send
   * to the evaluator with the largest amount of data to receive without regard to types of data.
   * @param optimizedEvaluators a list of optimized evaluators.
   * @return the generated plan.
   */
  private Plan generatePlan(final List<OptimizedEvaluator> optimizedEvaluators) {
    final PlanImpl.Builder builder = PlanImpl.newBuilder();
    final PriorityQueue<OptimizedEvaluator> senderPriorityQueue =
        new PriorityQueue<>(optimizedEvaluators.size(), ascendingComparator);
    final PriorityQueue<OptimizedEvaluator> receiverPriorityQueue =
        new PriorityQueue<>(optimizedEvaluators.size(), descendingComparator);

    for (final OptimizedEvaluator evaluator : optimizedEvaluators) {
      final int dataUnitToMove = getDataUnitsToMove(evaluator);
      if (dataUnitToMove < 0) {
        senderPriorityQueue.add(evaluator);
      } else if (dataUnitToMove > 0) {
        receiverPriorityQueue.add(evaluator);
      } else {
        continue;
      }

      if (evaluator.getParticipateValue() && evaluator.getId().startsWith(NEW_TEMP_EVALUATOR_ID_PREFIX)) {
        evaluator.setId(getNewEvaluatorId()); // replace temporary id with new one.
        builder.addEvaluatorToAdd(evaluator.getId());
        System.out.println("add evaluator: " + evaluator.getId());
      } else if (!evaluator.getParticipateValue() && !evaluator.getId().startsWith(NEW_TEMP_EVALUATOR_ID_PREFIX)) {
        builder.addEvaluatorToDelete(evaluator.getId());
        System.out.println("delete evaluator: " + evaluator.getId());
      }
    }

    //debug
    System.out.println("init sender queue size=" + senderPriorityQueue.size());
    System.out.println("init receiver queue size=" + receiverPriorityQueue.size());

    while (senderPriorityQueue.size() > 0) {
      final OptimizedEvaluator sender = senderPriorityQueue.poll();
      System.out.println("after sender popped, queue size=" + senderPriorityQueue.size());
      while (getDataUnitsToMove(sender) < 0) {
        final OptimizedEvaluator receiver = receiverPriorityQueue.poll();
        System.out.println("after receiver popped queue size=" + receiverPriorityQueue.size());
        builder.addTransferSteps(generateTransferStep(sender, receiver));
        if (getDataUnitsToMove(receiver) > 0) {
          receiverPriorityQueue.add(receiver);
        }
      }
    }
    return builder.build();
  }

  /**
   * Returns the number of data units to move.
   * If a return value is positive, the specified evaluator should receive data from others.
   * If a return value is negative, the evaluator should send data to others.
   * @param evaluator
   * @return the number of data units to move.
   */
  private static int getDataUnitsToMove(final OptimizedEvaluator evaluator) {
    return evaluator.getRequestedDataValue() - getSumDataUnits(evaluator.getAllocatedDataInfos());
  }

  /**
   * Generates {@link TransferStep} from one evaluator to another evaluator.
   * @param sender the evaluator which sends data.
   * @param receiver the evaluator which receives data.
   * @return the generated {@link TransferStep}.
   */
  private List<TransferStep> generateTransferStep(final OptimizedEvaluator sender, final OptimizedEvaluator receiver) {
    final List<TransferStep> ret = new ArrayList<>();
    int numToSend = getDataUnitsToMove(sender);
    if (numToSend >= 0) {
      throw new IllegalArgumentException("The number of data units to send must be < 0");
    }
    int numToReceive = getDataUnitsToMove(receiver);
    System.out.println(String.format("numToSend=%d numToRecv=%d", numToSend, numToReceive));

    final List<DataInfo> dataInfosToRemove = new ArrayList<>();
    final List<DataInfo> dataInfosToAdd = new ArrayList<>(1);
    final Iterator<DataInfo> dataInfoIterator = sender.getAllocatedDataInfos().iterator();

    while (numToSend < 0 && numToReceive > 0 && dataInfoIterator.hasNext()) {
      final DataInfo dataInfo = dataInfoIterator.next();
      final DataInfo dataInfoToMove;

      if (numToReceive < dataInfo.getNumUnits()) {
        dataInfoToMove = new DataInfoImpl(dataInfo.getDataType(), numToReceive);
        dataInfosToAdd.add(new DataInfoImpl(dataInfo.getDataType(), dataInfo.getNumUnits() - numToReceive));
      } else {
        dataInfoToMove = dataInfo;
      }

      numToSend += dataInfoToMove.getNumUnits();
      numToReceive -= dataInfoToMove.getNumUnits();

      dataInfosToRemove.add(dataInfo);
      receiver.getAllocatedDataInfos().add(dataInfoToMove);
      ret.add(new TransferStepImpl(sender.getId(), receiver.getId(), dataInfoToMove));
    }

    sender.getAllocatedDataInfos().removeAll(dataInfosToRemove);
    sender.getAllocatedDataInfos().addAll(dataInfosToAdd);

    //debug
    System.out.println(ret);

    return ret;
  }

  /**
   * Predicts computing performance of new evaluators by averaging computing performance of others.
   * @param optimizedEvaluators
   * @return the predicted computing performance.
   */
  private static double predictComputePerformance(final List<OptimizedEvaluator> optimizedEvaluators) {
    double sum = 0D;
    int count = 0;
    for (final OptimizedEvaluator evaluator : optimizedEvaluators) {
      final int sumDataUnits = getSumDataUnits(evaluator.getAllocatedDataInfos());
      if (sumDataUnits > 0) {
        sum += evaluator.getComputeCost() / sumDataUnits;
        ++count;
      }
    }
    return sum / count;
  }

  private String getNewEvaluatorId() {
    return NEW_EVALUATOR_ID_PREFIX + newEvaluatorSequence.getAndIncrement();
  }

  private static class OptimizedEvaluator {
    private String id;
    private final Variable participateVariable;
    private final Variable requestedDataVariable;
    private final Collection<DataInfo> allocatedDataInfos;
    private final double computeCost;

    OptimizedEvaluator(final String id,
                       final Variable participateVariable,
                       final Variable requestedDataVariable) {
      this.id = id;
      this.participateVariable = participateVariable;
      this.requestedDataVariable = requestedDataVariable;
      this.allocatedDataInfos = new ArrayList<>(0);
      this.computeCost = 0D;
    }

    OptimizedEvaluator(final String id,
                       final Variable participateVariable,
                       final Variable requestedDataVariable,
                       final Collection<DataInfo> dataInfos,
                       final double computeCost) {
      this.id = id;
      this.participateVariable = participateVariable;
      this.requestedDataVariable = requestedDataVariable;
      this.allocatedDataInfos = dataInfos;
      this.computeCost = computeCost;
    }

    public String getId() {
      return id;
    }

    public void setId(final String newId) {
      id = newId;
    }

    public Variable getParticipateVariable() {
      return participateVariable;
    }

    public boolean getParticipateValue() {
      return participateVariable.getValue().intValue() == 1;
    }

    public Variable getRequestedDataVariable() {
      return requestedDataVariable;
    }

    public int getRequestedDataValue() {
      return requestedDataVariable.getValue().intValue();
    }

    public Collection<DataInfo> getAllocatedDataInfos() {
      return allocatedDataInfos;
    }

    public double getComputeCost() {
      return computeCost;
    }
  }

  /**
   * Comparator class that compares two {@link edu.snu.cay.dolphin.core.optimizer.ILPSolverOptimizer.OptimizedEvaluator}
   * with the number of data units for each evaluator to move.
   */
  private static class DataUnitsToMoveComparator implements Comparator<OptimizedEvaluator> {

    public enum Order { ASCENDING, DESCENDING }

    private final Order order;

    public DataUnitsToMoveComparator() {
      this.order = Order.ASCENDING;
    }
    public DataUnitsToMoveComparator(final Order order) {
      this.order = order;
    }

    @Override
    public int compare(final OptimizedEvaluator o1, final OptimizedEvaluator o2) {
      final int o1DataDiff = getDataUnitsToMove(o1);
      final int o2DataDiff = getDataUnitsToMove(o2);
      final int ret = Integer.compare(o1DataDiff, o2DataDiff);
      if (order == Order.DESCENDING) {
        return -1 * ret;
      } else {
        return ret;
      }
    }
  }
}
