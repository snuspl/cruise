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
package edu.snu.cay.services.em.optimizer.impl;

import edu.snu.cay.services.em.optimizer.api.DataInfo;
import edu.snu.cay.services.em.optimizer.api.EvaluatorParameters;
import edu.snu.cay.services.em.plan.api.Plan;
import edu.snu.cay.services.em.optimizer.api.Optimizer;
import edu.snu.cay.services.em.plan.impl.PlanImpl;
import edu.snu.cay.services.em.plan.api.TransferStep;
import edu.snu.cay.services.em.plan.impl.TransferStepImpl;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * An Optimizer implementation that creates a Random plan.
 *
 * The plan is generated by:
 * 1. Choosing uniformly between [minEvaluatorsFraction * availableEvaluators,
 *    maxEvaluatorsFraction * availableEvaluators] evaluators to use in the plan.
 *    {@link #getEvaluatorsToUse}
 * 2. Randomly redistributing all blocks -- by batching blocks (of 10) and
 *    sending each batch to an evaluator selected uniformly at random.
 *    {@link #distributeDataAcrossEvaluators}
 * 3. Creating transfer steps by greedily taking data needed to
 *    satisfy the request from an evaluator from it's right side (in a circular array).
 *    {@link #getTransferSteps}
 */
public final class RandomOptimizer implements Optimizer {

  @NamedParameter(short_name = "random_optimizer_min_fraction",
      default_value = "0.5", doc = "The minimum fraction of available evaluators to use. Range [0, 1.0]")
  public static final class MinEvaluatorsFraction implements Name<Double> {
  }

  @NamedParameter(short_name = "random_optimizer_max_fraction",
      default_value = "1.0", doc = "The maximum fraction of available evaluators to use. Range [0, 1.0]")
  public static final class MaxEvaluatorsFraction implements Name<Double> {
  }

  private static final Logger LOG = Logger.getLogger(RandomOptimizer.class.getName());

  private final Random random = new Random();
  private final AtomicInteger newEvaluatorId = new AtomicInteger(0);

  private final double minEvaluatorsFraction;
  private final double maxEvaluatorsFraction;

  @Inject
  private RandomOptimizer(@Parameter(MinEvaluatorsFraction.class) final double minEvaluatorsFraction,
                          @Parameter(MaxEvaluatorsFraction.class) final double maxEvaluatorsFraction) {
    if (minEvaluatorsFraction < 0.0 || minEvaluatorsFraction > 1.0
        || maxEvaluatorsFraction < 0.0 || maxEvaluatorsFraction > 1.0) {
      throw new IllegalArgumentException(
          "minEvaluatorsFraction " + minEvaluatorsFraction
              + " and maxEvaluatorsFraction " + maxEvaluatorsFraction
              + " must be within range [0.0, 1.0]");
    }

    this.minEvaluatorsFraction = minEvaluatorsFraction;
    this.maxEvaluatorsFraction = maxEvaluatorsFraction;
  }

  @Override
  public Plan optimize(final Map<String, List<EvaluatorParameters>> evalParamsMap,
                       final int availableEvaluators) {
    LOG.log(Level.INFO, "EvaluatorParameters: {0}, Available Evaluators: {1}",
        new Object[]{evalParamsMap, availableEvaluators});

    if (availableEvaluators <= 0) {
      throw new IllegalArgumentException("availableEvaluators " + availableEvaluators + " must be > 0");
    }

    final int numNamespace = evalParamsMap.size();

    // allocate evaluators evenly for each name space
    final int numEvaluators = getEvaluatorsToUse(availableEvaluators) / numNamespace;

    final PlanImpl.Builder planBuilder = PlanImpl.newBuilder();

    for (final String namespace : evalParamsMap.keySet()) {

      final List<EvaluatorParameters> evaluatorsToAdd;
      final List<EvaluatorParameters> evaluatorsToDelete;
      final List<EvaluatorParameters> activeEvaluators = new ArrayList<>(evalParamsMap.get(namespace));
      if (numEvaluators > activeEvaluators.size()) {
        evaluatorsToDelete = new ArrayList<>(0);
        evaluatorsToAdd = getNewEvaluators(numEvaluators - activeEvaluators.size()); // Add to the tail
      } else if (numEvaluators < activeEvaluators.size()) {
        evaluatorsToAdd = new ArrayList<>(0);
        evaluatorsToDelete = new ArrayList<>(
            activeEvaluators.subList(numEvaluators, activeEvaluators.size())); // Delete from the tail
      } else {
        evaluatorsToAdd = new ArrayList<>(0);
        evaluatorsToDelete = new ArrayList<>(0);
      }

      planBuilder.addEvaluatorsToAdd(namespace, getIds(evaluatorsToAdd));
      planBuilder.addEvaluatorsToDelete(namespace, getIds(evaluatorsToDelete));

    /*
     * 1. Find the sum of the number of blocks across the Evaluators. (getSumData)
     * 2. Initialize an OptimizedEvaluator per Evaluator from the DataInfo. (initOptimizedEvaluators)
     *    An OptimizedEvaluator keeps track of data at an Evaluator while distributing data and scheduling transfers.
     * 3. Randomly distribute all blocks across the non-deleted Evaluators. (distributeDataAcrossEvaluators)
     * 4. Create transfer steps to satisfy the distribution of blocks. (getTransferSteps)
      *   Add these transfer steps to the plan.
     */
      activeEvaluators.addAll(evaluatorsToAdd);

      final long sumDataBlocks = getSumDataBlocks(activeEvaluators);
      final List<OptimizedEvaluator> evaluators = initOptimizedEvaluators(activeEvaluators);
      distributeDataAcrossEvaluators(evaluators.subList(0, numEvaluators), sumDataBlocks);

      if (LOG.isLoggable(Level.FINE)) {
        for (final OptimizedEvaluator evaluator : evaluators) {
          LOG.log(Level.FINE, "RandomOptimizer data distribution: {0} {1}",
              new Object[]{evaluator.id, evaluator.dataRequested});
        }
      }

      final List<TransferStep> transferSteps = getTransferSteps(evaluators);
      for (final TransferStep transferStep : transferSteps) {
        planBuilder.addTransferStep(namespace, transferStep);
      }
    }
    final Plan plan = planBuilder.build();
    LOG.log(Level.FINE, "RandomOptimizer Plan: {0}", plan);
    return plan;
  }

  private int getEvaluatorsToUse(final int availableEvaluators) {
    final int minEvaluators = (int) (availableEvaluators * minEvaluatorsFraction);
    final int maxEvaluators = (int) (availableEvaluators * maxEvaluatorsFraction);

    if (maxEvaluators - minEvaluators == 0) {
      return availableEvaluators;
    } else {
      return minEvaluators + random.nextInt(maxEvaluators - minEvaluators + 1); // + 1 makes this inclusive
    }
  }

  private List<EvaluatorParameters> getNewEvaluators(final int numEvaluators) {
    final List<EvaluatorParameters> newEvaluators = new ArrayList<>();
    for (int i = 0; i < numEvaluators; i++) {
      newEvaluators.add(getNewEvaluator());
    }
    return newEvaluators;
  }

  private EvaluatorParameters getNewEvaluator() {
    return new EvaluatorParametersImpl("newRandomOptimizerNode-" + newEvaluatorId.getAndIncrement(),
        new DataInfoImpl(), new HashMap<>(0));
  }

  private static long getSumDataBlocks(final Collection<EvaluatorParameters> evaluators) {
    long sumDataBlocks = 0;
    for (final EvaluatorParameters evaluator : evaluators) {
      sumDataBlocks += evaluator.getDataInfo().getNumBlocks();
    }
    return sumDataBlocks;
  }

  /**
   * Initialize an OptimizedEvaluator per Evaluator from the DataInfo.
   * An OptimizedEvaluator keeps track of data at an Evaluator while distributing data and scheduling transfers.
   * @param evaluators EvaluatorParameters to create OptimizedEvaluators for
   * @return list of OptimizedEvaluator's, one per EvaluatorParameter passed in
   */
  private static List<OptimizedEvaluator> initOptimizedEvaluators(final Collection<EvaluatorParameters> evaluators) {
    final List<OptimizedEvaluator> optimizedEvaluators = new ArrayList<>(evaluators.size());
    optimizedEvaluators.addAll(
        evaluators.stream()
            .map(parameters -> new OptimizedEvaluator(parameters.getId(), parameters.getDataInfo()))
            .collect(Collectors.toList()));
    return optimizedEvaluators;
  }

  private static List<String> getIds(final Collection<EvaluatorParameters> evaluators) {
    final List<String> ids = new ArrayList<>(evaluators.size());
    for (final EvaluatorParameters evaluator : evaluators) {
      ids.add(evaluator.getId());
    }
    return ids;
  }

  /**
   * Randomly redistribute all data blocks.
   * The distribution is done by batching blocks (of 10) and
   * sending each batch to an evaluator selected uniformly at random.
   * @param evaluators Evaluators to distribute data to. Should *not* include to-be-deleted Evaluators.
   * @param totalData the amount of data to distribute
   */
  private void distributeDataAcrossEvaluators(final List<OptimizedEvaluator> evaluators,
                                              final long totalData) {
    // Add each unit of data to a random evaluator.
    final int unit = 10;

    // Do remainder first
    final int remainder = (int) totalData % unit;
    evaluators.get(getRandomEvaluatorIndex(evaluators.size())).setDataRequested(remainder);

    // Do the rest in unit increments
    for (long i = 0; i < totalData / unit; i++) {
      final OptimizedEvaluator evaluator = evaluators.get(getRandomEvaluatorIndex(evaluators.size()));
      evaluator.setDataRequested(evaluator.getDataRequested() + unit);
    }
  }

  private int getRandomEvaluatorIndex(final int numEvaluators) {
    return random.nextInt(numEvaluators);
  }

  /**
   * Create transfer steps to satisfy redistributed data at the evaluators.
   * Iterate through the evaluators. When a evaluator needs more data, take it from the right side (circular).
   * @param evaluators Evaluators that can participate in transfers. *Should* include to-be-deleted Evaluators.
   * @return list of transfer steps
   */
  private List<TransferStep> getTransferSteps(final List<OptimizedEvaluator> evaluators) {
    final List<TransferStep> transferSteps = new ArrayList<>();

    for (int i = 0; i < evaluators.size(); i++) {
      final OptimizedEvaluator dstEvaluator = evaluators.get(i);
      if (dstEvaluator.getDataRemaining() > 0) {
        // Find srcEvaluator's with data blocks to transfer to dstEvaluator,
        // starting from the next index and iterating through as a circular array.
        for (int j = 1; j < evaluators.size(); j++) {
          final OptimizedEvaluator srcEvaluator = evaluators.get((i + j) % evaluators.size());
          if (srcEvaluator.getDataRemaining() < 0) {
            // Mark all available blocks at srcEvaluator as transfer steps to be transferred to dstEvaluator.
            final int dataToTransfer = Math.min(dstEvaluator.getDataRemaining(), 0 - srcEvaluator.getDataRemaining());
            srcEvaluator.sendData(dstEvaluator.getId(), dataToTransfer);
            dstEvaluator.receiveData(srcEvaluator.getId(), dataToTransfer);
          }
          if (dstEvaluator.getDataRemaining() == 0) {
            break;
          }
        }
      }
      if (dstEvaluator.getDataRemaining() > 0) {
        for (final OptimizedEvaluator evaluator : evaluators) {
          LOG.log(Level.SEVERE, evaluator.toString());
        }

        throw new RuntimeException("Unable to satisfy data request. Still need to receive "
            + dstEvaluator.getDataRemaining());
      }
      transferSteps.addAll(dstEvaluator.getDstTransferSteps());
    }

    return transferSteps;
  }

  private static class OptimizedEvaluator {
    private final String id;
    private final List<TransferStep> dstTransferSteps = new ArrayList<>();
    private int dataAllocated;
    private int dataRequested;

    public OptimizedEvaluator(final String id, final DataInfo dataInfo) {
      this.id = id;
      this.dataAllocated = dataInfo.getNumBlocks();
      this.dataRequested = 0;
    }

    public OptimizedEvaluator(final String id) {
      this.id = id;
      this.dataAllocated = 0;
      this.dataRequested = 0;
    }

    public String getId() {
      return id;
    }

    public int getDataRequested() {
      return dataRequested;
    }

    public void setDataRequested(final int dataRequested) {
      this.dataRequested = dataRequested;
    }

    public int getDataAllocated() {
      return dataAllocated;
    }

    public int getDataRemaining() {
      return dataRequested - dataAllocated;
    }

    public void sendData(final String dst, final int data) {
      dataAllocated -= data;
    }

    public void receiveData(final String src, final int data) {
      dataAllocated += data;
      dstTransferSteps.add(new TransferStepImpl(src, id, new DataInfoImpl(data)));
    }

    public List<TransferStep> getDstTransferSteps() {
      return dstTransferSteps;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("OptimizedEvaluator{");
      sb.append("id='").append(id).append('\'');
      sb.append(", dstTransferSteps=").append(dstTransferSteps);
      sb.append(", dataAllocated=").append(dataAllocated);
      sb.append(", dataRequested=").append(dataRequested);
      sb.append('}');
      return sb.toString();
    }
  }
}
