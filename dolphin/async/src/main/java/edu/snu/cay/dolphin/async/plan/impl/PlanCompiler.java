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
package edu.snu.cay.dolphin.async.plan.impl;

import edu.snu.cay.dolphin.async.*;
import edu.snu.cay.dolphin.async.plan.api.Plan;
import edu.snu.cay.dolphin.async.plan.api.TransferStep;
import edu.snu.cay.services.et.configuration.ExecutorConfiguration;
import edu.snu.cay.services.et.plan.api.Op;
import edu.snu.cay.services.et.plan.impl.ETPlan;
import edu.snu.cay.services.et.plan.impl.op.*;
import edu.snu.cay.utils.DAG;
import edu.snu.cay.utils.DAGImpl;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.cay.dolphin.async.optimizer.parameters.Constants.NAMESPACE_WORKER;
import static edu.snu.cay.dolphin.async.optimizer.parameters.Constants.NAMESPACE_SERVER;

/**
 * A plan compiler that compiles down Dolphin's plan to {@link ETPlan}.
 */
public final class PlanCompiler {
  private static final Logger LOG = Logger.getLogger(PlanCompiler.class.getName());

  private final String modelTableId;
  private final String inputTableId;

  private final InjectionFuture<DolphinDriver> dolphinDriverFuture;
  private final InjectionFuture<DolphinMaster> dolphinMasterFuture;

  @Inject
  private PlanCompiler(@Parameter(DolphinParameters.ModelTableId.class) final String modelTableId,
                       @Parameter(DolphinParameters.InputTableId.class) final String inputTableId,
                       final InjectionFuture<DolphinDriver> dolphinDriverFuture,
                       final InjectionFuture<DolphinMaster> dolphinMasterFuture) {
    this.modelTableId = modelTableId;
    this.inputTableId = inputTableId;
    this.dolphinDriverFuture = dolphinDriverFuture;
    this.dolphinMasterFuture = dolphinMasterFuture;
  }

  /**
   * Translate given collections of add/del operations into transferstep list, by pairing each add and del operations.
   * When paring, it finds add/del ops that have the same evaluator id.
   * It generates a list of transferstep to apply change in this translation.
   * @param evalsToDel a list of del
   * @param evalsToAdd a list of add
   * @param transferSteps a list of transferstep
   * @return a pair of executor Ids to switch and transfersteps that match with switch
   */
  private Pair<List<String>, List<TransferStep>> translateToSwitch(final List<String> evalsToDel,
                                                                   final List<String> evalsToAdd,
                                                                   final List<TransferStep> transferSteps) {
    final Map<String, String> addIdToDelId = new HashMap<>();
    final List<String> delSublist = new LinkedList<>();
    
    // Find machines that will be switched.
    for (final String evalId : evalsToDel) {
      if (evalsToAdd.contains(evalId)) {
        addIdToDelId.put(evalId, evalId);
        evalsToAdd.remove(evalId);
        delSublist.add(evalId);
      }
    }
    evalsToDel.removeAll(delSublist);
  
    final List<String> executorIdsToSwitch = new ArrayList<>(delSublist);
    delSublist.clear();
    
    final List<TransferStep> transferStepForSwitch = new ArrayList<>(transferSteps.size());
    for (final TransferStep transferStep : transferSteps) {
      // Change the destination of TransferSteps to the executors that will be switched,
      // if the TransferSteps were planned to move data to the executors that will be added.
      if (addIdToDelId.containsKey(transferStep.getDstId())) {
        transferStepForSwitch.add(
            new TransferStepImpl(transferStep.getSrcId(),
                addIdToDelId.get(transferStep.getDstId()),
                transferStep.getDataInfo()));
      } else {
        transferStepForSwitch.add(transferStep);
      }
    }

    return Pair.of(executorIdsToSwitch, transferStepForSwitch);
  }


  /**
   * Translate given collections of add/del operations into transferstep list, by pairing each add and del operations.
   * When paring, it does not care the evaluator id of add/del ops, and only considers the {@code numEvalsToSwitch}.
   * It generates a list of transferstep to apply change in this translation.
   * @param evalsToDel a list of del
   * @param evalsToAdd a list of add
   * @param transferSteps a list of transferstep
   * @param numEvalsToSwitch the number of executors to switch
   * @return a pair of executor Ids to switch and transfersteps that match with switch
   */
  private Pair<List<String>, List<TransferStep>> translateToSwitch(final List<String> evalsToDel,
                                                                   final List<String> evalsToAdd,
                                                                   final List<TransferStep> transferSteps,
                                                                   final int numEvalsToSwitch) {
    final Map<String, String> addIdToDelId = new HashMap<>(numEvalsToSwitch);
    final List<String> delSublist = evalsToDel.subList(0, numEvalsToSwitch);
    final List<String> addSubList = evalsToAdd.subList(0, numEvalsToSwitch);
    for (int idx = 0; idx < numEvalsToSwitch; idx++) {
      addIdToDelId.put(addSubList.get(idx), delSublist.get(idx));
    }

    final List<String> executorIdsToSwitch = new ArrayList<>(delSublist);
    delSublist.clear();
    addSubList.clear();

    final List<TransferStep> transferStepForSwitch = new ArrayList<>(transferSteps.size());
    for (final TransferStep transferStep : transferSteps) {
      // Change the destination of TransferSteps to the executors that will be switched,
      // if the TransferSteps were planned to move data to the executors that will be added.
      if (addIdToDelId.containsKey(transferStep.getDstId())) {
        transferStepForSwitch.add(
            new TransferStepImpl(transferStep.getSrcId(),
                addIdToDelId.get(transferStep.getDstId()),
                transferStep.getDataInfo()));
      } else {
        transferStepForSwitch.add(transferStep);
      }
    }

    return Pair.of(executorIdsToSwitch, transferStepForSwitch);
  }

  /**
   * Compiles a Dolphin's plan to {@link ETPlan}.
   * @param dolphinPlan a Dolphin's plan
   * @param numAvailableExtraEvals the number of extra executors
   * @return a {@link ETPlan}
   */
  public ETPlan compile(final Plan dolphinPlan, final int numAvailableExtraEvals) {
    final Map<String, Collection<String>> srcNamespaceToEvalsToSwitch = new HashMap<>();

    final List<String> serversToDel = new ArrayList<>(dolphinPlan.getEvaluatorsToDelete(NAMESPACE_SERVER));
    final List<String> workersToDel = new ArrayList<>(dolphinPlan.getEvaluatorsToDelete(NAMESPACE_WORKER));
    final List<String> serversToAdd = new ArrayList<>(dolphinPlan.getEvaluatorsToAdd(NAMESPACE_SERVER));
    final List<String> workersToAdd = new ArrayList<>(dolphinPlan.getEvaluatorsToAdd(NAMESPACE_WORKER));
    List<TransferStep> serverTransferSteps = new ArrayList<>(dolphinPlan.getTransferSteps(NAMESPACE_SERVER));
    List<TransferStep> workerTransferSteps = new ArrayList<>(dolphinPlan.getTransferSteps(NAMESPACE_WORKER));

    // We have two switch translations here. (Ordering is important!)
    // The first is for a pair of add/del that each has the same target eval id.
    // It's for {@link ILPOptimizer}, which already knows that add/del op will be translated into switch op.

    // The second translation does not about the eval id of each add/del operation.
    // It just picks add/del ops randomly to eliminate a pair of add/del in different namespace.
    // It's for all other existing optimizers.
    // Actually in this case, optimizers do not specify meaningful eval id for add op,
    // because they think add op is for acquiring a 'new' resource and
    // the newly allocated eval's id will be assigned by RM or REEF.

    // First switch translation.
    final Pair<List<String>, List<TransferStep>> evalIdsToTransfersForSwitch0 =
        translateToSwitch(serversToDel, workersToAdd, workerTransferSteps); // server -> worker

    srcNamespaceToEvalsToSwitch.put(NAMESPACE_SERVER, evalIdsToTransfersForSwitch0.getLeft());
    workerTransferSteps = evalIdsToTransfersForSwitch0.getRight();

    final Pair<List<String>, List<TransferStep>> evalIdsToTransfersForSwitch1 =
        translateToSwitch(workersToDel, serversToAdd, serverTransferSteps); // worker -> server

    srcNamespaceToEvalsToSwitch.put(NAMESPACE_WORKER, evalIdsToTransfersForSwitch1.getLeft());
    serverTransferSteps = evalIdsToTransfersForSwitch1.getRight();


    final int numSwitchesFromServerToWorker = Math.min(workersToAdd.size(), serversToDel.size());
    final int numSwitchesFromWorkerToServer = Math.min(serversToAdd.size(), workersToDel.size());

    // Second switch translation.
    if (numSwitchesFromServerToWorker > 0) { // server -> worker
      final Pair<List<String>, List<TransferStep>> evalIdsToTransfersForSwitch =
          translateToSwitch(serversToDel, workersToAdd, workerTransferSteps, numSwitchesFromServerToWorker);

      srcNamespaceToEvalsToSwitch.put(NAMESPACE_SERVER, evalIdsToTransfersForSwitch.getLeft());
      workerTransferSteps = evalIdsToTransfersForSwitch.getRight();
    }
    if (numSwitchesFromWorkerToServer > 0) { // worker -> server
      final Pair<List<String>, List<TransferStep>> evalIdsToTransfersForSwitch =
          translateToSwitch(workersToDel, serversToAdd, serverTransferSteps, numSwitchesFromWorkerToServer);

      srcNamespaceToEvalsToSwitch.put(NAMESPACE_WORKER, evalIdsToTransfersForSwitch.getLeft());
      serverTransferSteps = evalIdsToTransfersForSwitch.getRight();
    }

    final Map<String, Collection<String>> namespaceToEvalsToAdd = new HashMap<>();
    namespaceToEvalsToAdd.put(NAMESPACE_WORKER, workersToAdd);
    namespaceToEvalsToAdd.put(NAMESPACE_SERVER, serversToAdd);

    final Map<String, Collection<String>> namespaceToEvalsToDel = new HashMap<>();
    namespaceToEvalsToDel.put(NAMESPACE_WORKER, workersToDel);
    namespaceToEvalsToDel.put(NAMESPACE_SERVER, serversToDel);

    final Map<String, Collection<TransferStep>> namespaceToTransferSteps = new HashMap<>();
    namespaceToTransferSteps.put(NAMESPACE_WORKER, workerTransferSteps);
    namespaceToTransferSteps.put(NAMESPACE_SERVER, serverTransferSteps);

    return buildPlan(namespaceToEvalsToAdd, namespaceToEvalsToDel, srcNamespaceToEvalsToSwitch,
        namespaceToTransferSteps, numAvailableExtraEvals);
  }

  private ETPlan buildPlan(final Map<String, Collection<String>> namespaceToEvalsToAdd,
                           final Map<String, Collection<String>> namespaceToEvalsToDel,
                           final Map<String, Collection<String>> srcNamespaceToEvalsToSwitch,
                           final Map<String, Collection<TransferStep>> namespaceToTransferSteps,
                           final int numAvailableExtraEvals) {
    final DAG<Op> dag = new DAGImpl<>();

    final Map<String, AllocateOp> allocateOps = new HashMap<>();
    final Map<String, DeallocateOp> deallocateOps = new HashMap<>();

    final Map<String, StartOp> startOps = new HashMap<>();
    final Map<String, StopOp> stopOps = new HashMap<>();

    final Map<String, AssociateOp> associateOps = new HashMap<>();
    final Map<String, UnassociateOp> unassociateOps = new HashMap<>();

    final Map<String, SubscribeOp> subscribeOps = new HashMap<>();
    final Map<String, UnsubscribeOp> unsubscribeOps = new HashMap<>();

    final List<MoveOp> moveOps = new LinkedList<>();

    handleSwitch(srcNamespaceToEvalsToSwitch, dag,
        startOps, stopOps, associateOps, unassociateOps, subscribeOps, unsubscribeOps);

    handleDelete(namespaceToEvalsToDel, dag, deallocateOps, stopOps, unassociateOps, unsubscribeOps);
    
    handleAdd(namespaceToEvalsToAdd, dag, allocateOps, startOps, associateOps, subscribeOps);

    // below two steps should be done after above steps.
    resolveAddDelDependency(numAvailableExtraEvals, dag, allocateOps, deallocateOps);

    handleMove(namespaceToTransferSteps, dag, associateOps, unassociateOps, moveOps, stopOps, startOps);

    final int numTotalOps = allocateOps.size() + deallocateOps.size()
        + moveOps.size()
        + startOps.size() + stopOps.size()
        + associateOps.size() + unassociateOps.size()
        + subscribeOps.size() + unsubscribeOps.size();

    return new ETPlan(dag, numTotalOps);
  }

  private void resolveAddDelDependency(final int numAvailableExtraEvals,
                                       final DAG<Op> dag,
                                       final Map<String, AllocateOp> allocateOps,
                                       final Map<String, DeallocateOp> deallocateOps) {
    // We need one Delete for each Add as much as the number of extra evaluators slots
    // is smaller than the number of evaluators to Add.
    // The current strategy simply maps one Delete and one Add that is not necessarily relevant with.
    final int numRequiredExtraEvals = allocateOps.size() - deallocateOps.size();
    if (numRequiredExtraEvals > numAvailableExtraEvals) {
      throw new RuntimeException("Infeasible plan; it tries to use more resources than allowed.");
    }

    final int numAddsShouldFollowDel = allocateOps.size() - numAvailableExtraEvals;
    if (numAddsShouldFollowDel > 0) {
      LOG.log(Level.FINE, "{0} Allocates should follow the same number of Deallocates.", numAddsShouldFollowDel);

      final Iterator<DeallocateOp> deallocateOpsItr = deallocateOps.values().iterator();
      final Iterator<AllocateOp> allocateOpsItr = allocateOps.values().iterator();

      // pick each add/del operations with no special ordering
      for (int i = 0; i < numAddsShouldFollowDel; i++) {
        final Op addOp = allocateOpsItr.next();
        final Op delOp = deallocateOpsItr.next();
        dag.addEdge(delOp, addOp);
      }
    }
  }

  private void handleSwitch(final Map<String, Collection<String>> srcNamespaceToEvalsToSwitch,
                            final DAG<Op> dag,
                            final Map<String, StartOp> startOps,
                            final Map<String, StopOp> stopOps,
                            final Map<String, AssociateOp> associateOps,
                            final Map<String, UnassociateOp> unassociateOps,
                            final Map<String, SubscribeOp> subscribeOps,
                            final Map<String, UnsubscribeOp> unsubscribeOps) {
    // only workers need Start and Stop ops
    for (final Map.Entry<String, Collection<String>> entry : srcNamespaceToEvalsToSwitch.entrySet()) {
      final String srcNamespace = entry.getKey();
      final Collection<String> executors = entry.getValue();

      // server -> worker:
      // Stop (S) -> Unassociate (M) -> Subscribe (M) -> Start (W)
      //                                Associate (I) ->
      if (srcNamespace.equals(NAMESPACE_SERVER)) {
        for (final String executor : executors) {
          final StopOp serverStopOp = new StopOp(executor);
          stopOps.put(executor, serverStopOp);
          dag.addVertex(serverStopOp);

          final UnassociateOp unassociateOp = new UnassociateOp(executor, modelTableId);
          unassociateOps.put(executor, unassociateOp);
          dag.addVertex(unassociateOp);
          dag.addEdge(serverStopOp, unassociateOp);

          final SubscribeOp subscribeOp = new SubscribeOp(executor, modelTableId);
          subscribeOps.put(executor, subscribeOp);
          dag.addVertex(subscribeOp);
          dag.addEdge(unassociateOp, subscribeOp);

          final AssociateOp associateOp = new AssociateOp(executor, inputTableId);
          associateOps.put(executor, associateOp);
          dag.addVertex(associateOp);

          final StartOp workerStartOp = new StartOp(executor, dolphinMasterFuture.get().getWorkerTaskConf(),
              dolphinMasterFuture.get().getWorkerMetricConf());
          startOps.put(executor, workerStartOp);
          dag.addVertex(workerStartOp);
          dag.addEdge(associateOp, workerStartOp);
          dag.addEdge(subscribeOp, workerStartOp);
        }

        // worker -> server:
        // Stop (W) -> Unsubscribe (M) -> Associate (M) -> Start (S)
        //          -> Unassociate (I)
      } else {
        for (final String executor : executors) {
          final StopOp workerStopOp = new StopOp(executor);
          stopOps.put(executor, workerStopOp);
          dag.addVertex(workerStopOp);

          final UnsubscribeOp unsubscribeOp = new UnsubscribeOp(executor, modelTableId);
          unsubscribeOps.put(executor, unsubscribeOp);
          dag.addVertex(unsubscribeOp);
          dag.addEdge(workerStopOp, unsubscribeOp);

          final UnassociateOp unassociateOp = new UnassociateOp(executor, inputTableId);
          unassociateOps.put(executor, unassociateOp);
          dag.addVertex(unassociateOp);
          dag.addEdge(workerStopOp, unassociateOp);

          final AssociateOp associateOp = new AssociateOp(executor, modelTableId);
          associateOps.put(executor, associateOp);
          dag.addVertex(associateOp);
          dag.addEdge(unsubscribeOp, associateOp);

          final StartOp serverStartOp = new StartOp(executor, dolphinMasterFuture.get().getServerTaskConf(),
              dolphinMasterFuture.get().getServerMetricConf());
          startOps.put(executor, serverStartOp);
          dag.addVertex(serverStartOp);
          dag.addEdge(associateOp, serverStartOp);
        }
      }
    }
  }

  private void handleDelete(final Map<String, Collection<String>> namespaceToEvalsToDel,
                            final DAG<Op> dag,
                            final Map<String, DeallocateOp> deallocateOps,
                            final Map<String, StopOp> stopOps,
                            final Map<String, UnassociateOp> unassociateOps,
                            final Map<String, UnsubscribeOp> unsubscribeOps) {
    for (final Map.Entry<String, Collection<String>> entry : namespaceToEvalsToDel.entrySet()) {
      final String namespace = entry.getKey();

      final String tableIdToUnassociate = namespace.equals(NAMESPACE_WORKER) ? inputTableId : modelTableId;

      final Collection<String> evalsToDel = entry.getValue();
      for (final String evalToDel : evalsToDel) {
        final DeallocateOp deallocateOp = new DeallocateOp(evalToDel);
        deallocateOps.put(evalToDel, deallocateOp);
        dag.addVertex(deallocateOp);

        final UnassociateOp unassociateOp = new UnassociateOp(evalToDel, tableIdToUnassociate);
        unassociateOps.put(evalToDel, unassociateOp);
        dag.addVertex(unassociateOp);
        dag.addEdge(unassociateOp, deallocateOp);

        if (namespace.equals(NAMESPACE_WORKER)) {
          final StopOp stopOp = new StopOp(evalToDel);
          stopOps.put(evalToDel, stopOp);
          final UnsubscribeOp unsubscribeOp = new UnsubscribeOp(evalToDel, modelTableId);
          unsubscribeOps.put(evalToDel, unsubscribeOp);

          dag.addVertex(stopOp);
          dag.addVertex(unsubscribeOp);
          dag.addEdge(stopOp, unsubscribeOp);
          dag.addEdge(unsubscribeOp, deallocateOp);
        } else {
          final StopOp stopOp = new StopOp(evalToDel);
          stopOps.put(evalToDel, stopOp);

          dag.addVertex(stopOp);
          dag.addEdge(stopOp, deallocateOp);
        }
      }
    }
  }

  private void handleAdd(final Map<String, Collection<String>> namespaceToEvalsToAdd,
                         final DAG<Op> dag,
                         final Map<String, AllocateOp> allocateOps,
                         final Map<String, StartOp> startOps,
                         final Map<String, AssociateOp> associateOps,
                         final Map<String, SubscribeOp> subscribeOps)  {
    for (final Map.Entry<String, Collection<String>> entry : namespaceToEvalsToAdd.entrySet()) {
      final String namespace = entry.getKey();
      final Collection<String> evalsToAdd = entry.getValue();

      final ExecutorConfiguration executorConf = namespace.equals(NAMESPACE_WORKER) ?
          dolphinDriverFuture.get().getWorkerExecutorConf() : dolphinDriverFuture.get().getServerExecutorConf();
      final String tableIdToAssociate = namespace.equals(NAMESPACE_WORKER) ? inputTableId : modelTableId;

      for (final String evalToAdd : evalsToAdd) {
        final AllocateOp allocateOp = new AllocateOp(evalToAdd, executorConf);
        allocateOps.put(evalToAdd, allocateOp);
        dag.addVertex(allocateOp);

        final AssociateOp associateOp = new AssociateOp(evalToAdd, tableIdToAssociate);
        associateOps.put(evalToAdd, associateOp);
        dag.addVertex(associateOp);
        dag.addEdge(allocateOp, associateOp);

        if (namespace.equals(NAMESPACE_WORKER)) {
          final StartOp startOp = new StartOp(evalToAdd,
              dolphinMasterFuture.get().getWorkerTaskConf(),
              dolphinMasterFuture.get().getWorkerMetricConf());
          startOps.put(evalToAdd, startOp);
          final SubscribeOp subscribeOp = new SubscribeOp(evalToAdd, modelTableId);
          subscribeOps.put(evalToAdd, subscribeOp);

          dag.addVertex(startOp);
          dag.addVertex(subscribeOp);
          dag.addEdge(allocateOp, subscribeOp);
          dag.addEdge(subscribeOp, startOp);
        } else {
          final StartOp startOp = new StartOp(evalToAdd,
              dolphinMasterFuture.get().getServerTaskConf(),
              dolphinMasterFuture.get().getServerMetricConf());
          startOps.put(evalToAdd, startOp);

          dag.addVertex(startOp);
          dag.addEdge(allocateOp, startOp);
        }
      }
    }
  }

  private void handleMove(final Map<String, Collection<TransferStep>> namespaceToTransferSteps,
                          final DAG<Op> dag,
                          final Map<String, AssociateOp> associateOps, final Map<String, UnassociateOp> unassociateOps,
                          final List<MoveOp> moveOps,
                          final Map<String, StopOp> stopOps, final Map<String, StartOp> startOps) {
    // add vertices of Move
    for (final Map.Entry<String, Collection<TransferStep>> entry : namespaceToTransferSteps.entrySet()) {
      final String namespace = entry.getKey();
      final Collection<TransferStep> transferSteps = entry.getValue();

      final String tableId = namespace.equals(NAMESPACE_WORKER) ? inputTableId : modelTableId;

      for (final TransferStep transferStep : transferSteps) {
        final MoveOp moveOp = new MoveOp(transferStep.getSrcId(), transferStep.getDstId(),
            tableId, transferStep.getDataInfo().getNumBlocks());
        moveOps.add(moveOp);
        dag.addVertex(moveOp);
      }
    }

    // resolve dependencies of Move
    for (final MoveOp moveOp : moveOps) {
      final String srcId = moveOp.getSrcExecutorId();
      final String dstId = moveOp.getDstExecutorId();

      // associate -> move dependency
      if (associateOps.containsKey(dstId)) {
        final AssociateOp associateOp = associateOps.get(dstId);
        dag.addEdge(associateOp, moveOp);
      }

      // move -> start dependency
      if (startOps.containsKey(dstId)) {
        final StartOp startOp = startOps.get(dstId);
        dag.addEdge(moveOp, startOp);
      }

      // move -> unassociate dependency
      if (unassociateOps.containsKey(srcId)) {
        final UnassociateOp unassociateOp = unassociateOps.get(srcId);
        dag.addEdge(moveOp, unassociateOp);
      }

      // stop -> move dependency
      if (stopOps.containsKey(srcId)) {
        final StopOp stopOp = stopOps.get(srcId);
        dag.addEdge(stopOp, moveOp);
      }
    }
  }
}
