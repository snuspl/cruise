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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An Optimizer implementation that creates a Random plan.
 *
 * The plan is generated by:
 * 1. Choosing uniformly between [minEvaluatorsFraction * availableEvaluators,
 *    maxEvaluatorsFraction * availableEvaluators] evaluators to use in the plan.
 *    {@link #getEvaluatorsToUse}
 * 2. For each dataType, randomly redistributing all units -- by batching units (of 10) and
 *    sending each batch to an evaluator selected uniformly at random.
 *    {@link #uniformlyRandomDataRequestPerEvaluator}
 * 3. For each dataType, creating transfer steps by greedily taking data needed to
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
  public Plan optimize(final Collection<EvaluatorParameters> activeEvaluators, final int availableEvaluators) {
    if (availableEvaluators <= 0) {
      throw new IllegalArgumentException("availableEvaluators " + availableEvaluators + " must be > 0");
    }

    final int numEvaluators = getEvaluatorsToUse(availableEvaluators);

    final List<EvaluatorParameters> evaluatorsToAdd;
    final List<EvaluatorParameters> evaluatorsToDelete;
    final List<EvaluatorParameters> activeEvaluatorsList = new ArrayList<>(activeEvaluators);
    if (numEvaluators > activeEvaluatorsList.size()) {
      evaluatorsToDelete = new ArrayList<>(0);
      evaluatorsToAdd = getNewEvaluators(numEvaluators - activeEvaluatorsList.size()); // Add to the tail
    } else {
      evaluatorsToAdd = new ArrayList<>(0);
      evaluatorsToDelete = new ArrayList<>(
          activeEvaluatorsList.subList(numEvaluators, activeEvaluatorsList.size())); // Delete from the tail
    }

    final PlanImpl.Builder planBuilder = PlanImpl.newBuilder()
        .addEvaluatorsToAdd(getIds(evaluatorsToAdd))
        .addEvaluatorsToDelete(getIds(evaluatorsToDelete));

    activeEvaluatorsList.addAll(evaluatorsToAdd);
    final Collection<String> dataTypes = getDataTypes(activeEvaluatorsList);
    for (final String dataType : dataTypes) {

      long sumData = 0;
      for (final EvaluatorParameters evaluator : activeEvaluatorsList) {
        for (final DataInfo dataInfo : evaluator.getDataInfos()) {
          if (dataType.equals(dataInfo.getDataType())) {
            sumData += dataInfo.getNumUnits();
          }
        }
      }

      final List<OptimizedEvaluator> evaluators = getWrappedEvaluators(dataType, activeEvaluatorsList);
      uniformlyRandomDataRequestPerEvaluator(evaluators.subList(0, numEvaluators), sumData);

      for (final OptimizedEvaluator evaluator : evaluators) {
        LOG.log(Level.FINE, evaluator.toString());
      }

      final List<TransferStep> transferSteps = getTransferSteps(evaluators);
      planBuilder.addTransferSteps(transferSteps);

    }
    return planBuilder.build();
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
      newEvaluators.add(getNewEvaluator(i));
    }
    return newEvaluators;
  }

  private EvaluatorParameters getNewEvaluator(final int index) {
    return new EvaluatorParametersImpl("new-" + random.nextInt() + "-" + index, new ArrayList<DataInfo>(0));
  }

  private static Collection<String> getDataTypes(final Collection<EvaluatorParameters> evaluators) {
    final Set<String> dataTypes = new HashSet<>();
    for (final EvaluatorParameters evaluator : evaluators) {
      for (final DataInfo dataInfo : evaluator.getDataInfos()) {
        dataTypes.add(dataInfo.getDataType());
      }
    }
    return dataTypes;
  }

  private static List<OptimizedEvaluator> getWrappedEvaluators(final String dataType,
                                                               final Collection<EvaluatorParameters> evaluators) {
    final List<OptimizedEvaluator> wrappedEvaluators = new ArrayList<>(evaluators.size());
    for (final EvaluatorParameters parameters : evaluators) {
      boolean dataTypeAdded = false;
      for (final DataInfo dataInfo : parameters.getDataInfos()) {
        if (dataType.equals(dataInfo.getDataType())) {
          if (dataTypeAdded) {
            throw new IllegalArgumentException("Cannot have multiple infos for " + dataType);
          }
          wrappedEvaluators.add(new OptimizedEvaluator(parameters.getId(), dataInfo));
          dataTypeAdded = true;
        }
      }
      if (!dataTypeAdded) {
        wrappedEvaluators.add(new OptimizedEvaluator(parameters.getId(), dataType));
      }
    }
    return wrappedEvaluators;
  }

  private static List<String> getIds(final Collection<EvaluatorParameters> evaluators) {
    final List<String> ids = new ArrayList<>(evaluators.size());
    for (final EvaluatorParameters evaluator : evaluators) {
      ids.add(evaluator.getId());
    }
    return ids;
  }

  private void uniformlyRandomDataRequestPerEvaluator(final List<OptimizedEvaluator> evaluators,
                                                      final long totalData) {
    // Add each unit of data to a random evaluator.
    final int unit = 10;

    // Do remainder first
    final int remainder = (int) totalData % unit;
    evaluators.get(randomEvaluatorIndex(evaluators.size())).setDataRequested(remainder);

    // Do the rest in unit increments
    for (long i = 0; i < totalData / unit; i++) {
      final OptimizedEvaluator evaluator = evaluators.get(randomEvaluatorIndex(evaluators.size()));
      evaluator.setDataRequested(evaluator.getDataRequested() + unit);
    }
  }

  /**
   * Iterate through the evaluators. When a evaluator needs more data, take it from the right side (circular).
   */
  private List<TransferStep> getTransferSteps(final List<OptimizedEvaluator> evaluators) {
    final List<TransferStep> transferSteps = new ArrayList<>();

    for (int i = 0; i < evaluators.size(); i++) {
      final OptimizedEvaluator dstEvaluator = evaluators.get(i);
      if (dstEvaluator.getDataRemaining() > 0) {
        for (int j = 1; j < evaluators.size(); j++) {
          final OptimizedEvaluator srcEvaluator = evaluators.get((i + j) % evaluators.size());
          if (srcEvaluator.getDataRemaining() < 0) {
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

  private int randomEvaluatorIndex(final int numEvaluators) {
    return random.nextInt(numEvaluators);
  }

  private static class OptimizedEvaluator {
    private final String id;
    private final String dataType;
    private final List<TransferStep> dstTransferSteps = new ArrayList<>();
    private int dataAllocated;
    private int dataRequested;

    public OptimizedEvaluator(final String id, final DataInfo dataInfo) {
      this.id = id;
      this.dataType = dataInfo.getDataType();
      this.dataAllocated = dataInfo.getNumUnits();
      this.dataRequested = 0;
    }

    public OptimizedEvaluator(final String id, final String dataType) {
      this.id = id;
      this.dataType = dataType;
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
      dstTransferSteps.add(new TransferStepImpl(src, id, new DataInfoImpl(dataType, data)));
    }

    public List<TransferStep> getDstTransferSteps() {
      return dstTransferSteps;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("OptimizedEvaluator{");
      sb.append("id='").append(id).append('\'');
      sb.append(", dataType='").append(dataType).append('\'');
      sb.append(", dstTransferSteps=").append(dstTransferSteps);
      sb.append(", dataAllocated=").append(dataAllocated);
      sb.append(", dataRequested=").append(dataRequested);
      sb.append('}');
      return sb.toString();
    }
  }
}
