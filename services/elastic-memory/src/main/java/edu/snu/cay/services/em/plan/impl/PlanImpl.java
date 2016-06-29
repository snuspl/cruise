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
package edu.snu.cay.services.em.plan.impl;

import edu.snu.cay.services.em.plan.api.TransferStep;
import edu.snu.cay.services.em.plan.api.Plan;
import edu.snu.cay.utils.DAG;
import edu.snu.cay.utils.DAGImpl;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A plan implementation with builder.
 * The builder checks the feasibility of plan and dependencies between detailed steps.
 */
public final class PlanImpl implements Plan {
  private static final Logger LOG = Logger.getLogger(PlanImpl.class.getName());

  private final Map<String, Set<String>> evaluatorsToAdd;
  private final Map<String, Set<String>> evaluatorsToDelete;
  private final Map<String, List<TransferStep>> allTransferSteps;

  private final DAG<EMOperation> dependencyGraph;
  private final int numTotalOperations;

  private PlanImpl(final Map<String, Set<String>> evaluatorsToAdd,
                   final Map<String, Set<String>> evaluatorsToDelete,
                   final Map<String, List<TransferStep>> allTransferSteps,
                   final DAG<EMOperation> dependencyGraph) {
    this.evaluatorsToAdd = evaluatorsToAdd;
    this.evaluatorsToDelete = evaluatorsToDelete;
    this.allTransferSteps = allTransferSteps;
    this.dependencyGraph = dependencyGraph;

    // count the total number of operations
    int numTotalOps = 0;
    for (final Set<String> evalsToAdd : evaluatorsToAdd.values()) {
      numTotalOps += evalsToAdd.size();
    }
    for (final Set<String> evalsToDel : evaluatorsToDelete.values()) {
      numTotalOps += evalsToDel.size();
    }
    for (final List<TransferStep> transferSteps : allTransferSteps.values()) {
      numTotalOps += transferSteps.size();
    }
    this.numTotalOperations = numTotalOps;
  }

  @Override
  public int getPlanSize() {
    return numTotalOperations;
  }

  @Override
  public synchronized Set<EMOperation> getReadyOps() {
    return new HashSet<>(dependencyGraph.getRootVertices());
  }

  @Override
  public synchronized Set<EMOperation> onComplete(final EMOperation operation) {
    final Set<EMOperation> newAvaliableOperations = new HashSet<>(dependencyGraph.getNeighbors(operation));
    dependencyGraph.removeVertex(operation);

    for (final EMOperation candidate : newAvaliableOperations) {
      if (dependencyGraph.getInDegree(candidate) > 0) {
        newAvaliableOperations.remove(candidate);
      }
    }
    return newAvaliableOperations;
  }

  @Override
  public Collection<String> getEvaluatorsToAdd(final String namespace) {
    if (!evaluatorsToAdd.containsKey(namespace)) {
      return Collections.emptyList();
    }
    return evaluatorsToAdd.get(namespace);
  }

  @Override
  public Collection<String> getEvaluatorsToDelete(final String namespace) {
    if (!evaluatorsToDelete.containsKey(namespace)) {
      return Collections.emptyList();
    }
    return evaluatorsToDelete.get(namespace);
  }

  @Override
  public Collection<TransferStep> getTransferSteps(final String namespace) {
    if (!allTransferSteps.containsKey(namespace)) {
      return Collections.emptyList();
    }
    return allTransferSteps.get(namespace);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("PlanImpl{");
    for (final String key : evaluatorsToAdd.keySet()) {
      sb.append("evaluatorsToAdd=(").append(key).append(',').append(evaluatorsToAdd.get(key)).append(')');
    }
    for (final String key : evaluatorsToDelete.keySet()) {
      sb.append("evaluatorsToDelete=(").append(key).append(',').append(evaluatorsToDelete.get(key)).append(')');
    }
    for (final String key : allTransferSteps.keySet()) {
      sb.append("TransferSteps=(").append(key).append(',').append(allTransferSteps.get(key)).append(')');
    }
    sb.append('}');
    return sb.toString();
  }

  public static PlanImpl.Builder newBuilder() {
    return new Builder();
  }

  /**
   * A builder of PlanImpl.
   * Before instantiating a PlanImpl object, it checks the feasibility of plan and
   * constructs a dependency graph between steps, based on the following rules:
   *   1. Evaluators must be added before they participate in transfers. (add -> move)
   *   2. Evaluators must finish all transfers which they are a part of before they are deleted. (move -> del)
   *   3. One evaluator must be deleted before adding a new evaluator,
   *     because we are using the whole available resources. (del -> add)
   */
  public static final class Builder implements org.apache.reef.util.Builder<PlanImpl> {
    private final Map<String, Set<String>> evaluatorsToAdd = new HashMap<>();
    private final Map<String, Set<String>> evaluatorsToDelete = new HashMap<>();
    private final Map<String, List<TransferStep>> allTransferSteps = new HashMap<>();

    private Builder() {
    }

    public Builder addEvaluatorToAdd(final String namespace, final String evaluatorId) {
      if (!evaluatorsToAdd.containsKey(namespace)) {
        evaluatorsToAdd.put(namespace, new HashSet<>());
      }
      final Set<String> evaluatorIds = evaluatorsToAdd.get(namespace);
      evaluatorIds.add(evaluatorId);
      return this;
    }

    public Builder addEvaluatorsToAdd(final String namespace, final Collection<String> evaluatorIdsToAdd) {
      if (!evaluatorsToAdd.containsKey(namespace)) {
        evaluatorsToAdd.put(namespace, new HashSet<>());
      }
      final Set<String> evaluatorIds = evaluatorsToAdd.get(namespace);
      evaluatorIds.addAll(evaluatorIdsToAdd);
      return this;
    }

    public Builder addEvaluatorToDelete(final String namespace, final String evaluatorId) {
      if (!evaluatorsToDelete.containsKey(namespace)) {
        evaluatorsToDelete.put(namespace, new HashSet<>());
      }
      final Set<String> evaluatorIds = evaluatorsToDelete.get(namespace);
      evaluatorIds.add(evaluatorId);
      return this;
    }

    public Builder addEvaluatorsToDelete(final String namespace, final Collection<String> evaluatorIdsToDelete) {
      if (!evaluatorsToDelete.containsKey(namespace)) {
        evaluatorsToDelete.put(namespace, new HashSet<>());
      }
      final Set<String> evaluatorIds = evaluatorsToDelete.get(namespace);
      evaluatorIds.addAll(evaluatorIdsToDelete);
      return this;
    }

    public Builder addTransferStep(final String namespace, final TransferStep transferStep) {
      if (!allTransferSteps.containsKey(namespace)) {
        allTransferSteps.put(namespace, new ArrayList<>());
      }
      final List<TransferStep> transferStepList = allTransferSteps.get(namespace);
      transferStepList.add(transferStep);
      return this;
    }

    public Builder addTransferSteps(final String namespace, final Collection<TransferStep> transferSteps) {
      if (!allTransferSteps.containsKey(namespace)) {
        allTransferSteps.put(namespace, new ArrayList<>());
      }
      final List<TransferStep> transferStepList = allTransferSteps.get(namespace);
      transferStepList.addAll(transferSteps);
      return this;
    }

    @Override
    public PlanImpl build() {
      // check the observance of invariants in the plan (details are described below)

      // 1. do not allow the use of same evaluator id for both in Add and Delete in the same namespace
      // note that we do not care the uniqueness of evaluator id across namespaces
      for (final String namespace : evaluatorsToAdd.keySet()) {
        for (final String evaluator : evaluatorsToAdd.get(namespace)) {
          if (evaluatorsToDelete.containsKey(namespace) &&
              evaluatorsToDelete.get(namespace).contains(evaluator)) {
            throw new RuntimeException(evaluator + " is planned for both addition and deletion.");
          }
        }
      }

      // 2. do not allow moving out/in of data from/to evaluators that will be added/deleted in a single plan
      for (final String namespace : allTransferSteps.keySet()) {
        for (final TransferStep transferStep : allTransferSteps.get(namespace)) {
          if (evaluatorsToDelete.containsKey(namespace) &&
              evaluatorsToDelete.get(namespace).contains(transferStep.getDstId())) {
            throw new RuntimeException(transferStep.getDstId() + " is planned for deletion.");
          } else if (evaluatorsToAdd.containsKey(namespace) &&
              evaluatorsToAdd.get(namespace).contains(transferStep.getSrcId())) {
            throw new RuntimeException(transferStep.getSrcId() + " is planned for addition.");
          }
        }
      }

      // build an execution graph considering dependency between steps
      final DAG<EMOperation> dependencyGraph = constructDAG(evaluatorsToAdd, evaluatorsToDelete, allTransferSteps);

      return new PlanImpl(evaluatorsToAdd, evaluatorsToDelete, allTransferSteps, dependencyGraph);
    }

    /**
     * Constructs a directed acyclic graph based on the rules described in the comment of builder.
     */
    private DAG<EMOperation> constructDAG(final Map<String, Set<String>> namespaceToEvalsToAdd,
                                          final Map<String, Set<String>> namespaceToEvalsToDel,
                                          final Map<String, List<TransferStep>> namespaceToTransferSteps) {

      final DAG<EMOperation> dag = new DAGImpl<>();

      // add vertices of Delete
      final Map<String, EMOperation> delOperations = new HashMap<>();
      for (final Map.Entry<String, Set<String>> entry : namespaceToEvalsToDel.entrySet()) {
        final String namespace = entry.getKey();
        final Set<String> evalsToDel = entry.getValue();
        for (final String evalToDel : evalsToDel) {
          final EMOperation delOperation = new EMOperation(namespace, EMOperation.OpType.DEL, evalToDel);
          delOperations.put(evalToDel, delOperation);
          dag.addVertex(delOperation);
        }
      }

      // add vertices of Add
      final Map<String, EMOperation> addOperations = new HashMap<>();
      for (final Map.Entry<String, Set<String>> entry : namespaceToEvalsToAdd.entrySet()) {
        final String namespace = entry.getKey();
        final Set<String> evalsToAdd = entry.getValue();
        for (final String evalToAdd : evalsToAdd) {
          final EMOperation addOperation = new EMOperation(namespace, EMOperation.OpType.ADD, evalToAdd);
          addOperations.put(evalToAdd, addOperation);
          dag.addVertex(addOperation);
        }
      }

      // add vertices of Move
      final List<EMOperation> moveOperations = new LinkedList<>();
      for (final Map.Entry<String, List<TransferStep>> entry : namespaceToTransferSteps.entrySet()) {
        final String namespace = entry.getKey();
        final List<TransferStep> transferSteps = entry.getValue();

        for (final TransferStep transferStep : transferSteps) {
          final EMOperation moveOperation = new EMOperation(namespace, transferStep);
          moveOperations.add(moveOperation);
          dag.addVertex(moveOperation);
        }
      }

      // add edges representing dependencies between vertices

      // 1. del -> add
      // We need one Add for each Delete, because we assume that the job always use the whole available resources.
      // The current strategy simply maps one Delete and one Add that is not necessarily relevant with.
      final Iterator<EMOperation> delOperationsIter = delOperations.values().iterator();
      for (final EMOperation addOperation : addOperations.values()) {
        if (delOperationsIter.hasNext()) {
          final EMOperation delOperation = delOperationsIter.next();
          dag.addEdge(delOperation, addOperation);
        } else {
          LOG.log(Level.WARNING, "The number of Adds are larger than the one of Deletes." +
              " When executing the plan, this Add step would be blocked due to the resource limit. {0}", addOperation);
        }
      }

      for (final EMOperation moveOperation : moveOperations) {
        final TransferStep transferStep = moveOperation.getTransferStep().get();
        final String srcId = transferStep.getSrcId();
        final String dstId = transferStep.getDstId();

        // 2. add -> move
        if (addOperations.containsKey(dstId)) {
          final EMOperation addOperation = addOperations.get(dstId);
          dag.addEdge(addOperation, moveOperation);
        }

        // 3. move -> del
        if (delOperations.containsKey(srcId)) {
          final EMOperation delOperation = delOperations.get(srcId);
          dag.addEdge(moveOperation, delOperation);
        }
      }

      return dag;
    }
  }
}
