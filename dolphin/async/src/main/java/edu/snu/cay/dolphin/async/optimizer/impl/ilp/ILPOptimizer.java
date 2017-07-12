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

import edu.snu.cay.common.param.Parameters;
import edu.snu.cay.dolphin.async.optimizer.api.EvaluatorParameters;
import edu.snu.cay.dolphin.async.optimizer.api.Optimizer;
import edu.snu.cay.dolphin.async.optimizer.impl.BandwidthInfoParser;
import edu.snu.cay.dolphin.async.optimizer.impl.CoreInfoParser;
import edu.snu.cay.dolphin.async.optimizer.impl.ServerEvaluatorParameters;
import edu.snu.cay.dolphin.async.optimizer.impl.WorkerEvaluatorParameters;
import edu.snu.cay.dolphin.async.optimizer.parameters.Constants;
import edu.snu.cay.dolphin.async.plan.api.Plan;
import edu.snu.cay.dolphin.async.plan.api.TransferStep;
import edu.snu.cay.dolphin.async.plan.impl.EmptyPlan;
import edu.snu.cay.dolphin.async.plan.impl.ILPPlanDescriptor;
import edu.snu.cay.dolphin.async.plan.impl.PlanImpl;
import gurobi.GRBException;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;

/**
 * Solves Integer Linear problem to find the optimal configuration.
 */
public final class ILPOptimizer implements Optimizer {
  private static final int NUM_EMPTY_BLOCK = 0;
  private final double defNetworkBandwidth;
  private final int defCoreNum;
  private final double optBenefitThreshold;
  private final Map<String, Double> hostToBandwidth;
  private final Map<String, Integer> hostToCoreNum;
  private final ILPSolver ilpSolver;

  @Inject
  private ILPOptimizer(@Parameter(Parameters.DefaultNetworkBandwidth.class) final double defNetworkBandwidth,
                       @Parameter(Parameters.DefaultCoreNum.class) final int defCoreNum,
                       @Parameter(Parameters.OptimizationBenefitThreshold.class) final double optBenefitThreshold,
                       final ILPSolver ilpSolver,
                       final BandwidthInfoParser bandwitdthInfoParser,
                       final CoreInfoParser coreInfoParser) {
    this.defNetworkBandwidth = defNetworkBandwidth;
    this.defCoreNum = defCoreNum;
    this.optBenefitThreshold = optBenefitThreshold;
    this.hostToBandwidth = bandwitdthInfoParser.parseBandwidthInfo();
    this.hostToCoreNum = coreInfoParser.parseCoreInfo();
    this.ilpSolver = ilpSolver;
  }

  @Override
  public Plan optimize(final Map<String, List<EvaluatorParameters>> evalParamsMap,
                       final int availableEvaluators,
                       final Map<String, Double> optimizerModelParamsMap) {
    final int numTotalDataBlocks = (int) Math.round(optimizerModelParamsMap.get(Constants.NUM_DATA_BLOCKS));
    final int numTotalModelBlocks = (int) Math.round(optimizerModelParamsMap.get(Constants.NUM_MODEL_BLOCKS));
    final List<EvaluatorParameters> serverParams = evalParamsMap.get(Constants.NAMESPACE_SERVER);
    final List<MachineDescriptor> serverDescriptors = processServerParameters(serverParams);

    final List<EvaluatorParameters> workerParams = evalParamsMap.get(Constants.NAMESPACE_WORKER);
    final List<MachineDescriptor> workerDescriptors = processWorkerParameters(workerParams);

    assert availableEvaluators ==
        evalParamsMap.get(Constants.NAMESPACE_SERVER).size() + evalParamsMap.get(Constants.NAMESPACE_WORKER).size();
    // We don't consider dynamic availability for now.

    final int p = (int) Math.round(optimizerModelParamsMap.get(Constants.AVG_PULL_SIZE_PER_MINI_BATCH));

    final int n = evalParamsMap.get(Constants.NAMESPACE_SERVER).size()
        + evalParamsMap.get(Constants.NAMESPACE_WORKER).size();
    final int[] dOld = new int[n];
    final int[] mOld = new int[n];
    final int[] roleOld = new int[n];
    final double[] cWProc = new double[n];
    final double[] bandwidth = new double[n];
    final String[] evalIds = new String[n];

    double avgCWProcPerCore = 0.0;
    int totalCoreNum = 0;
    for (final MachineDescriptor workerDescriptor : workerDescriptors) {
      avgCWProcPerCore += workerDescriptor.getcWProc();
      totalCoreNum += hostToCoreNum.getOrDefault(workerDescriptor.getHostName(), defCoreNum);
    }
    avgCWProcPerCore /= (double) totalCoreNum;

    int idx = 0;
    for (final MachineDescriptor serverDescriptor : serverDescriptors) {
      roleOld[idx] = EvaluatorRole.SERVER.getValue();
      dOld[idx] = serverDescriptor.getNumTrainingDataBlocks();
      mOld[idx] = serverDescriptor.getNumModelBlocks();
      bandwidth[idx] = serverDescriptor.getBandwidth();
      evalIds[idx] = serverDescriptor.getId();
      cWProc[idx] = avgCWProcPerCore * hostToCoreNum.getOrDefault(serverDescriptor.getHostName(), defCoreNum);
      idx++;
    }

    System.out.println("n: " + n + " idx: " + idx);

    for (final MachineDescriptor workerDescriptor : workerDescriptors) {
      roleOld[idx] = EvaluatorRole.WORKER.getValue();
      dOld[idx] = workerDescriptor.getNumTrainingDataBlocks();
      mOld[idx] = workerDescriptor.getNumModelBlocks();
      bandwidth[idx] = workerDescriptor.getBandwidth();
      evalIds[idx] = workerDescriptor.getId();
      cWProc[idx] = workerDescriptor.getcWProc();
      idx++;
    }

    try {
      final ConfDescriptor optConfDescriptor =
          ilpSolver.optimize(n, numTotalDataBlocks, numTotalModelBlocks, p, cWProc, bandwidth);

      if (optConfDescriptor == null) {
        return new EmptyPlan();
      }

      final PlanImpl.Builder planBuilder = PlanImpl.newBuilder();

      // roleOpt[], dOpt[], mOpt[]
      final ILPPlanDescriptor planDescriptor = ILPPlanGenerator.generatePlanDescriptor(evalIds, roleOld, dOld, mOld,
          optConfDescriptor.getRole(), optConfDescriptor.getD(), optConfDescriptor.getM());
      final List<String> workerEvalIdxsToAdd = planDescriptor.getEvaluatorsToAdd(Constants.NAMESPACE_WORKER);
      final List<String> serverEvalIdxsToAdd = planDescriptor.getEvaluatorsToAdd(Constants.NAMESPACE_SERVER);
      planBuilder.addEvaluatorsToAdd(Constants.NAMESPACE_WORKER, workerEvalIdxsToAdd);
      planBuilder.addEvaluatorsToAdd(Constants.NAMESPACE_SERVER, serverEvalIdxsToAdd);

      final List<String> workerEvalIdxsToDelete = planDescriptor.getEvaluatorsToDelete(Constants.NAMESPACE_WORKER);
      final List<String> serverEvalIdxsToDelete = planDescriptor.getEvaluatorsToDelete(Constants.NAMESPACE_SERVER);
      planBuilder.addEvaluatorsToDelete(Constants.NAMESPACE_WORKER, workerEvalIdxsToDelete);
      planBuilder.addEvaluatorsToDelete(Constants.NAMESPACE_SERVER, serverEvalIdxsToDelete);

      final List<TransferStep> workerTransferSteps = planDescriptor.getTransferSteps(Constants.NAMESPACE_WORKER);
      final List<TransferStep> serverTransferSteps = planDescriptor.getTransferSteps(Constants.NAMESPACE_SERVER);
      planBuilder.addTransferSteps(Constants.NAMESPACE_WORKER, workerTransferSteps);
      planBuilder.addTransferSteps(Constants.NAMESPACE_SERVER, serverTransferSteps);

      return planBuilder.build();

    } catch (final GRBException e) {
      throw new RuntimeException(e);
    }
  }

  private List<MachineDescriptor> processWorkerParameters(final List<EvaluatorParameters> evalParamsList) {
    final List<MachineDescriptor> machineDescriptors = new ArrayList<>(evalParamsList.size());
    for (final EvaluatorParameters evalParams : evalParamsList) {
      final WorkerEvaluatorParameters workerEvalParams = (WorkerEvaluatorParameters) evalParams;
      final String id = workerEvalParams.getId();
      final int numDataBlocks = workerEvalParams.getDataInfo().getNumBlocks();
      final String hostname = workerEvalParams.getMetrics().getHostname().toString();
      final double bandwidth = hostToBandwidth.getOrDefault(hostname, defNetworkBandwidth) / 8D;
      final double wProc = workerEvalParams.getMetrics().getTotalCompTime();
      machineDescriptors.add(
          new MachineDescriptor(id, bandwidth, EvaluatorRole.WORKER, numDataBlocks, wProc, NUM_EMPTY_BLOCK, hostname));
    }
    return machineDescriptors;
  }

  private List<MachineDescriptor> processServerParameters(final List<EvaluatorParameters> evalParamsList) {
    final List<MachineDescriptor> machineDescriptors = new ArrayList<>(evalParamsList.size());
    for (final EvaluatorParameters evalParams : evalParamsList) {
      final ServerEvaluatorParameters serverEvalParams = (ServerEvaluatorParameters) evalParams;
      final String id = serverEvalParams.getId();
      final int numModelBlocks = serverEvalParams.getDataInfo().getNumBlocks();
      final String hostname = serverEvalParams.getMetrics().getHostname().toString();
      final double bandwidth = hostToBandwidth.getOrDefault(hostname, defNetworkBandwidth / 8D);
      machineDescriptors.add(
          new MachineDescriptor(id, bandwidth, EvaluatorRole.SERVER, NUM_EMPTY_BLOCK, -1.0, numModelBlocks, hostname));
    }
    return machineDescriptors;
  }

  class MachineDescriptor {
    private String id;
    private double bandwidth;
    private double cWProc;
    private EvaluatorRole role;

    private int numTrainingDataBlocks; // d
    private int numModelBlocks; // m
    
    private String hostname;

    MachineDescriptor(final double bandwidth) {
      this.bandwidth = bandwidth;
    }

    MachineDescriptor(final String id,
                      final double bandwidth,
                      final EvaluatorRole role,
                      final int numTrainingDataBlocks,
                      final double cWProc,
                      final int numModelDataBlocks,
                      final String hostname) {
      this.id = id;
      this.bandwidth = bandwidth;
      this.cWProc = cWProc;
      this.role = role;
      this.numTrainingDataBlocks = numTrainingDataBlocks;
      this.numModelBlocks = numModelDataBlocks;
      this.hostname = hostname;
    }

    String getId() {
      return id;
    }

    double getBandwidth() {
      return bandwidth;
    }

    double getcWProc() {
      return cWProc;
    }

    EvaluatorRole getEvaluatorRole() {
      return role;
    }

    int getNumTrainingDataBlocks() {
      return numTrainingDataBlocks;
    }

    int getNumModelBlocks() {
      return numModelBlocks;
    }
    
    String getHostName() {return hostname;}
  }
}

enum EvaluatorRole {
  WORKER(1), SERVER(0);

  private final int value;

  EvaluatorRole(final int value) {
    this.value = value;
  }

  int getValue() {
    return value;
  }
}
