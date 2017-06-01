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

import edu.snu.cay.common.dataloader.HdfsSplitFetcher;
import edu.snu.cay.common.dataloader.HdfsSplitInfo;
import edu.snu.cay.common.dataloader.HdfsSplitManager;
import edu.snu.cay.common.dataloader.TextInputFormat;
import edu.snu.cay.common.param.Parameters;
import edu.snu.cay.dolphin.async.optimizer.api.EvaluatorParameters;
import edu.snu.cay.dolphin.async.optimizer.api.Optimizer;
import edu.snu.cay.dolphin.async.optimizer.impl.ServerEvaluatorParameters;
import edu.snu.cay.dolphin.async.optimizer.impl.WorkerEvaluatorParameters;
import edu.snu.cay.dolphin.async.optimizer.parameters.Constants;
import edu.snu.cay.dolphin.async.plan.api.Plan;
import edu.snu.cay.dolphin.async.plan.api.TransferStep;
import edu.snu.cay.dolphin.async.plan.impl.ILPPlanDescriptor;
import edu.snu.cay.dolphin.async.plan.impl.PlanImpl;
import gurobi.GRBException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;

/**
 * Created by yunseong on 5/28/17.
 */
public final class ILPOptimizer implements Optimizer {
  private static final int NUM_EMPTY_BLOCK = 0;
  private final int numTotalDataBlocks;
  private final int numTotalModelBlocks;
  private final double defNetworkBandwidth;
  private final double optBenefitThreshold;
  private final Map<String, Double> hostToBandwidth;
  private final ILPSolver ilpSolver;
  private final ILPPlanGenerator ilpPlanGenerator;

  @Inject
  private ILPOptimizer(@Parameter(Parameters.DefaultNetworkBandwidth.class) final double defNetworkBandwidth,
                       @Parameter(Parameters.HostToBandwidthFilePath.class)
                       final String hostBandwidthFilePath,
                       @Parameter(Parameters.OptimizationBenefitThreshold.class)
                       final double optBenefitThreshold,
                       final ILPSolver ilpSolver,
                       final ILPPlanGenerator ilpPlanGenerator) {
    this.numTotalDataBlocks = 1024; // FIXME
    this.numTotalModelBlocks = 1024; // FIXME
    this.defNetworkBandwidth = defNetworkBandwidth;
    this.optBenefitThreshold = optBenefitThreshold;
    hostToBandwidth = parseBandwidthInfo(hostBandwidthFilePath);
    this.ilpSolver = ilpSolver;
    this.ilpPlanGenerator = ilpPlanGenerator;
  }

  @Override
  public Plan optimize(final Map<String, List<EvaluatorParameters>> evalParamsMap,
                       final int availableEvaluators,
                       final Map<String, Double> optimizerModelParamsMap) {
    // Translate the parameters into one large map containing all nodes' information (hostnameToMachineDescriptors)
    final Map<String, MachineDescriptor> hostnameToMachineDescriptors = new HashMap<>();

    final List<EvaluatorParameters> serverParams = evalParamsMap.get(Constants.NAMESPACE_SERVER);
    processServerParameters(serverParams, hostnameToMachineDescriptors);

    final List<EvaluatorParameters> workerParams = evalParamsMap.get(Constants.NAMESPACE_WORKER);
    processWorkerParameters(workerParams, hostnameToMachineDescriptors);

    // Specify which machines become (un)available in the dynamic resource availability
    final int[] d = new int[evalParamsMap.size()]; // The number of blocks per worker
    final int[] m = new int[evalParamsMap.size()]; // The number of blocks per server
    final int[] w = new int[evalParamsMap.size()]; // Whether the machines are worker
    final int[] s = new int[evalParamsMap.size()]; // Whether the machines are server

    /////////////////////////////
    /////     Constants     /////
    /////////////////////////////
    final int n = 10; // Number of total machines
    final int dTotal = 100; // Number of total blocks (batches)
    final int mTotal = 20; // Number of total partitions
    final int p = 1000; // Bytes per each partition
    final double[] cWProc = new double[n];
    final double[] bandwidth = new double[n];
    for (int i = 0; i < n; i++) {
      cWProc[i] = 10.0; // Cost (in time) to compute a batch in machine i
      bandwidth[i] = 200.0; // Bandwidth of machine i
    }

    try {

      ilpSolver.optimize(n, dTotal, mTotal, p, cWProc, bandwidth);
      final PlanImpl.Builder planBuilder = PlanImpl.newBuilder();

      // roleOpt[], dOpt[], mOpt[]
      final ILPPlanDescriptor planDescriptor = ilpPlanGenerator.generatePlanDescriptor(null, null, null, null, null, null);
      final List<Integer> workerEvalIdxsToAdd = planDescriptor.getEvaluatorsToAdd(Constants.NAMESPACE_WORKER);
      final List<Integer> serverEvalIdxsToAdd = planDescriptor.getEvaluatorsToAdd(Constants.NAMESPACE_SERVER);
      planBuilder.addEvaluatorsToAdd(Constants.NAMESPACE_WORKER, null);
      planBuilder.addEvaluatorsToAdd(Constants.NAMESPACE_SERVER, null);

      final List<Integer> workerEvalIdxsToDelete = planDescriptor.getEvaluatorsToDelete(Constants.NAMESPACE_WORKER);
      final List<Integer> serverEvalIdxsToDelete = planDescriptor.getEvaluatorsToDelete(Constants.NAMESPACE_SERVER);
      planBuilder.addEvaluatorsToDelete(Constants.NAMESPACE_WORKER, null);
      planBuilder.addEvaluatorsToDelete(Constants.NAMESPACE_SERVER, null);

      final List<TransferStep> workerTransferSteps = planDescriptor.getTransferSteps(Constants.NAMESPACE_WORKER);
      final List<TransferStep> serverTransferSteps = planDescriptor.getTransferSteps(Constants.NAMESPACE_SERVER);
      planBuilder.addTransferSteps(Constants.NAMESPACE_WORKER, workerTransferSteps);
      planBuilder.addTransferSteps(Constants.NAMESPACE_SERVER, serverTransferSteps);

      return planBuilder.build();

    } catch (final GRBException e) {
      throw new RuntimeException(e);
    }
  }

  private void processWorkerParameters(final List<EvaluatorParameters> evalParamsList,
                                       final Map<String, MachineDescriptor> hostnameToMachineDescriptors) {
     for (final EvaluatorParameters evalParams : evalParamsList) {
      final WorkerEvaluatorParameters workerEvalParams = (WorkerEvaluatorParameters) evalParams;
      final String id = workerEvalParams.getId();
      final int numDataBlocks = workerEvalParams.getDataInfo().getNumBlocks();
      final String hostname = workerEvalParams.getMetrics().getHostname().toString();
      final double bandwidth = hostToBandwidth.getOrDefault(hostname, defNetworkBandwidth);
      hostnameToMachineDescriptors.put(hostname, new MachineDescriptor(id, bandwidth, MachineDescriptor.MachineType.WORKER, numDataBlocks, NUM_EMPTY_BLOCK));
    }
  }

  private void processServerParameters(final List<EvaluatorParameters> evalParamsList,
                                       final Map<String, MachineDescriptor> hostnameToMachineDescriptors) {
    for (final EvaluatorParameters evalParams : evalParamsList) {
      final ServerEvaluatorParameters serverEvalParams = (ServerEvaluatorParameters) evalParams;
      final String id = serverEvalParams.getId();
      final int numModelBlocks = serverEvalParams.getDataInfo().getNumBlocks();
      final String hostname = serverEvalParams.getMetrics().getHostname().toString();
      final double bandwidth = hostToBandwidth.getOrDefault(hostname, defNetworkBandwidth);
      hostnameToMachineDescriptors.put(hostname, new MachineDescriptor(id, bandwidth, MachineDescriptor.MachineType.SERVER, NUM_EMPTY_BLOCK, numModelBlocks));
    }
  }

  /**
   * @param hostnameToBandwidthFilePath path of the file that consists of (hostname, bandwidth) information.
   * @return the mapping between the hostname and bandwidth of machines
   */
  private Map<String, Double> parseBandwidthInfo(final String hostnameToBandwidthFilePath) {
    if (hostnameToBandwidthFilePath.equals(Parameters.HostToBandwidthFilePath.NONE)) {
      return Collections.emptyMap();
    }

    final Map<String, Double> mapping = new HashMap<>();

    final HdfsSplitInfo[] infoArr =
        HdfsSplitManager.getSplits(hostnameToBandwidthFilePath, TextInputFormat.class.getName(), 1);

    assert infoArr.length == 1; // infoArr's length is always 1(NUM_SPLIT == 1).
    final HdfsSplitInfo info = infoArr[0];
    try {
      final Iterator<Pair<LongWritable, Text>> iterator = HdfsSplitFetcher.fetchData(info);
      while (iterator.hasNext()) {
        final String text = iterator.next().getValue().toString().trim();
        if (!text.startsWith("#") && text.length() != 0) { // comments and empty lines
          final String[] split = text.split("\\s+");
          assert split.length == 2;
          final String hostname = split[0];
          final double bandwidth = Double.parseDouble(split[1]);
          mapping.put(hostname, bandwidth);
        }
      }
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }

    return mapping;
  }

  private static class MachineDescriptor {
    private String id;
    private double bandwidth;
    enum MachineType {
      WORKER,
      SERVER,
      NONE
    }
    private MachineType machineType; // w|s
    private int numTrainingDataBlocks; // d
    private int numModelBlocks; // m

    MachineDescriptor(final double bandwidth) {
      this.bandwidth = bandwidth;
    }

    MachineDescriptor(final String id,
                      final double bandwidth,
                      final MachineType machineType,
                      final int numTrainingDataBlocks,
                      final int numModelDataBlocks) {
      this.id = id;
      this.bandwidth = bandwidth;
      this.machineType = machineType;
      this.numTrainingDataBlocks = numTrainingDataBlocks;
      this.numModelBlocks = numModelDataBlocks;
    }
  }
}
