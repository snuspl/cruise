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
package edu.snu.cay.services.et.examples.plan;

import edu.snu.cay.services.et.configuration.ExecutorConfiguration;
import edu.snu.cay.services.et.configuration.ResourceConfiguration;
import edu.snu.cay.services.et.configuration.TableConfiguration;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.ETMaster;
import edu.snu.cay.services.et.driver.impl.AllocatedTable;
import edu.snu.cay.services.et.examples.tableaccess.PrefixUpdateFunction;
import edu.snu.cay.services.et.exceptions.ExecutorNotExistException;
import edu.snu.cay.services.et.exceptions.PlanAlreadyExecutingException;
import edu.snu.cay.services.et.exceptions.TableNotExistException;
import edu.snu.cay.services.et.plan.api.Op;
import edu.snu.cay.services.et.plan.api.PlanExecutor;
import edu.snu.cay.services.et.plan.impl.ETPlan;
import edu.snu.cay.services.et.plan.impl.OpResult;
import edu.snu.cay.services.et.plan.impl.op.DeallocateOp;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Driver code for plan example.
 * Tests to check whether {@link PlanExecutor} works correctly.
 * Gets {@link ETPlan} from {@link ETPlanFactory}, and executes it.
 * In each tests, it compares the actual result with the expected result.
 */
@Unit
final class PlanETDriver {

  private static final Logger LOG = Logger.getLogger(PlanETDriver.class.getName());
  private static final int NUM_BLOCKS = 1024;
  private static final String EXECUTOR_ID_PREFIX = "Executor-";
  private static final String TABLE_ID = "Test-table";
  private static final String PLAN_LISTENER_ID = "PlanTestListener";
  static final int NUM_INITIAL_EXECUTORS = 3;


  private final ExecutorConfiguration executorConf;
  private final ETMaster etMaster;
  private final PlanExecutor planExecutor;
  private final ETPlanFactory etPlanFactory;
  private final AtomicInteger executorIdCounter = new AtomicInteger(4);

  @Inject
  private PlanETDriver(final ETMaster etMaster,
                       final PlanExecutor planExecutor,
                       final ETPlanFactory etPlanFactory) {
    this.etMaster = etMaster;
    this.planExecutor = planExecutor;
    this.executorConf = ExecutorConfiguration.newBuilder()
      .setResourceConf(
          ResourceConfiguration.newBuilder()
              .setNumCores(1)
              .setMemSizeInMB(128)
              .build())
      .build();
    this.etPlanFactory = etPlanFactory;
  }

  private TableConfiguration buildTableConf(final String tableId) {
    final TableConfiguration.Builder tableConfBuilder = TableConfiguration.newBuilder()
        .setId(tableId)
        .setNumTotalBlocks(NUM_BLOCKS)
        .setKeyCodecClass(SerializableCodec.class)
        .setValueCodecClass(SerializableCodec.class)
        .setUpdateValueCodecClass(SerializableCodec.class)
        .setUpdateFunctionClass(PrefixUpdateFunction.class)
        .setIsMutableTable(true)
        .setIsOrderedTable(true);

    return tableConfBuilder.build();
  }

  /**
   * Requests initial associators, and runs tests.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {

      try {
        final Future<List<AllocatedExecutor>> associatorsFuture =
            etMaster.addExecutors(NUM_INITIAL_EXECUTORS, executorConf);

        final List<AllocatedExecutor> associators = associatorsFuture.get();
        Executors.newSingleThreadExecutor().submit(() -> {
          try {
            final AllocatedTable table = etMaster.createTable(buildTableConf(TABLE_ID), associators).get();

            // 1. a test that allocate executors and deallocate them again
            runAllocateAndDeallocateTest(3);

            // 2. tests that only moves blocks between existing executors
            runMoveBlocksWithMultiDependency(associators, table);
            runMoveAllBlocksToOneExecutorPlan(associators.subList(1, associators.size()), associators.get(0), table);

            // 3. tests that (de)allocates an executor and moves blocks to/from it
            final AllocatedExecutor newAssociator = runAddOneAssociatorWithMovesTest(associators, TABLE_ID);
            associators.add(newAssociator);
            final AllocatedExecutor deletedExecutor = runDelOneAssociatorTest(associators, TABLE_ID);
            associators.remove(deletedExecutor);

            // close executors
            associators.forEach(AllocatedExecutor::close);

          } catch (InterruptedException | ExecutionException |
              PlanAlreadyExecutingException | TableNotExistException e) {
            throw new RuntimeException(e);
          }
        });
      } catch (Exception e) {
        LOG.log(Level.SEVERE, "unexpected exception " + e);
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Allocates executors and deallocates them again, by running two plans.
   * After executing a plan, confirm that the plan is executed as expected.
   * @param numExecutors the number of executors
   */
  private void runAllocateAndDeallocateTest(final int numExecutors)
      throws PlanAlreadyExecutingException, ExecutionException, InterruptedException {

    LOG.log(Level.INFO, "Allocate/Dellocate test is started. The number of executors to test with: {0}",
        numExecutors);

    final ETPlan allocPlan = etPlanFactory.getAllocateExecutorsPlan(numExecutors, executorConf);

    final List<AllocatedExecutor> allocatedExecutorList = new ArrayList<>(numExecutors);

    // it's an onetime-use listener, so unregister it after executing this plan
    planExecutor.registerListener(PLAN_LISTENER_ID, opResult -> {
      if (opResult.getType().equals(Op.OpType.ALLOCATE)) {
        final OpResult.AllocateOpResult allocateOpResult = (OpResult.AllocateOpResult) opResult;
        final AllocatedExecutor allocatedExecutor = allocateOpResult.getExecutor();
        allocatedExecutorList.add(allocatedExecutor);
      }
    });

    planExecutor.execute(allocPlan).get();

    planExecutor.unregisterListener(PLAN_LISTENER_ID);

    // check the result
    if (numExecutors != allocatedExecutorList.size()) {
      throw new RuntimeException(String.format("The actual number (%d) of allocated executor" +
          " is different from expectation (%d)", allocatedExecutorList.size(), numExecutors));
    }

    allocatedExecutorList.forEach(executor -> {
      try {
        etMaster.getExecutor(executor.getId());
      } catch (ExecutorNotExistException e) {
        throw new RuntimeException(e);
      }
    });

    LOG.log(Level.INFO, "Allocate test is completed.");

    // start deallocation test
    final Set<String> executorIds = allocatedExecutorList.stream()
        .map(AllocatedExecutor::getId).collect(Collectors.toSet());
    final ETPlan deallocPlan = etPlanFactory.getDeallocateExecutorsPlan(executorIds);

    final Set<String> deallocatedExecutorIds = new HashSet<>(executorIds.size());

    // it's an onetime-use listener, so unregister it after executing this plan
    planExecutor.registerListener(PLAN_LISTENER_ID, opResult -> {
      if (opResult.getType().equals(Op.OpType.DEALLOCATE)) {
        final DeallocateOp deallocateOp = (DeallocateOp) opResult.getOp();
        final String executorId = deallocateOp.getExecutorId();
        deallocatedExecutorIds.add(executorId);
      }
    });

    planExecutor.execute(deallocPlan).get();

    planExecutor.unregisterListener(PLAN_LISTENER_ID);

    // check the result
    if (!deallocatedExecutorIds.equals(executorIds)) {
      throw new RuntimeException(String.format("The expected set of deallocated executors: %s. The actual set: %s.",
          executorIds.toString(), deallocatedExecutorIds.toString()));
    }

    for (final String executorId : executorIds) {
      try {
        etMaster.getExecutor(executorId);
        throw new RuntimeException("Above method should throw the ExecutorNotExistException.");

      } catch (ExecutorNotExistException e) {
        // expected exception
      }
    }

    LOG.log(Level.INFO, "Deallocate test is completed.");
  }

  /**
   * Move table blocks between executors, with a plan that has a multiple stage dependency.
   * See {@link ETPlanFactory#getMoveBlocksWithMultiStagesPlan}.
   * @param executors a list of executors
   * @param table an associated table to move
   */
  private void runMoveBlocksWithMultiDependency(final List<AllocatedExecutor> executors,
                                                final AllocatedTable table) throws PlanAlreadyExecutingException,
      ExecutionException, InterruptedException {

    LOG.log(Level.INFO, "3. Moving blocks with multi dependency test is started");
    final Map<String, Integer> numBlocksMap = new HashMap<>();
    table.getPartitionInfo().entrySet().forEach(entry -> numBlocksMap.put(entry.getKey(), entry.getValue().size()));
    final ETPlan plan = etPlanFactory.getMoveBlocksWithMultiStagesPlan(executors, table.getId());
    planExecutor.execute(plan).get();

    table.getPartitionInfo().entrySet().forEach(entry -> {
      if (numBlocksMap.get(entry.getKey()) != entry.getValue().size()) {
        throw new RuntimeException("The number of blocks must be same");
      }
    });
    LOG.log(Level.INFO, "Moving blocks with multi dependency test is completed");
  }

  /**
   * Runs a plan that move all blocks in {@code srcExecutors} to {@code dstExecutor}.
   * @param srcExecutors a list of source executors
   * @param dstExecutor a destination executor
   * @param table an associated table to move
   */
  private void runMoveAllBlocksToOneExecutorPlan(final List<AllocatedExecutor> srcExecutors,
                                                 final AllocatedExecutor dstExecutor,
                                                 final AllocatedTable table) throws TableNotExistException,
      PlanAlreadyExecutingException, ExecutionException, InterruptedException {

    LOG.log(Level.INFO, "Moving all blocks to one executor test is started");

    final String targetExecutorId = dstExecutor.getId();
    final String tableId = table.getId();

    final Set<String> srcExecutorIds = srcExecutors.stream()
        .map(AllocatedExecutor::getId).collect(Collectors.toSet());

    final ETPlan plan = etPlanFactory.getMoveAllBlocksToOneExecutorPlan(srcExecutorIds, targetExecutorId, tableId);
    planExecutor.execute(plan).get();

    for (final Map.Entry<String, Set<Integer>> blocksEntry : table.getPartitionInfo().entrySet()) {
      final String executorId = blocksEntry.getKey();
      final int numBlocks = blocksEntry.getValue().size();

      if (executorId.equals(targetExecutorId)) {
        if (numBlocks != NUM_BLOCKS) {
          throw new RuntimeException("target executor must have a total number of blocks");
        }

      } else {
        if (numBlocks != 0) {
          throw new RuntimeException("rest executors must have a total number of blocks");
        }
      }
    }

    LOG.log(Level.INFO, "Moving all blocks to one executor test is completed");
  }

  /**
   * Runs a plan adding an additional associator to the table and moves some blocks to it.
   * @param existingAssociators a list of existing executors
   * @param tableId an associated table
   * @return a newly added associator
   */
  private AllocatedExecutor runAddOneAssociatorWithMovesTest(final List<AllocatedExecutor> existingAssociators,
                                                             final String tableId) throws PlanAlreadyExecutingException,
      ExecutionException, InterruptedException, TableNotExistException {

    LOG.log(Level.INFO, "runAddOneAssociatorTest is started");

    final String executorId = EXECUTOR_ID_PREFIX + executorIdCounter.getAndIncrement();
    final ETPlan plan = etPlanFactory.getAddOneAssociatorWithMovesPlan(
        executorId, tableId, executorConf, existingAssociators);

    final AtomicReference<AllocatedExecutor> newAssociator = new AtomicReference<>(null);

    // it's an onetime-use listener, so unregister it after executing this plan
    planExecutor.registerListener(PLAN_LISTENER_ID, opResult -> {
      if (opResult.getType().equals(Op.OpType.ALLOCATE)) {
        final OpResult.AllocateOpResult allocateOpResult = (OpResult.AllocateOpResult) opResult;
        final AllocatedExecutor allocatedExecutor = allocateOpResult.getExecutor();
        newAssociator.set(allocatedExecutor);
      }
    });
    planExecutor.execute(plan).get();

    planExecutor.unregisterListener(PLAN_LISTENER_ID);

    // check the result
    if (newAssociator.get() == null) {
      throw new RuntimeException();
    }

    try {
      etMaster.getExecutor(newAssociator.get().getId());
    } catch (ExecutorNotExistException e) {
      throw new RuntimeException(e);
    }

    final Set<Integer> blocksForNewAssociator = etMaster.getTable(tableId).getPartitionInfo()
        .get(newAssociator.get().getId());
    if (blocksForNewAssociator == null || blocksForNewAssociator.isEmpty()) {
      throw new RuntimeException();
    }

    LOG.log(Level.INFO, "runAddOneAssociatorTest is completed");
    return newAssociator.get();
  }

  /**
   * Runs a plan that deletes one associator from the table, moving all blocks from it.
   * @param executors a list of associators
   * @param tableId an associated table
   * @return a deleted associator
   */
  private AllocatedExecutor runDelOneAssociatorTest(final List<AllocatedExecutor> executors,
                                         final String tableId) throws PlanAlreadyExecutingException,
      ExecutionException, InterruptedException, TableNotExistException {

    LOG.log(Level.INFO, "runDelOneAssociatorTest is started");

    final ETPlan plan = etPlanFactory.getDelOneAssociatorWithMovesPlan(tableId, executors);

    final AtomicReference<String> deletedExecutorId = new AtomicReference<>(null);

    // it's an onetime-use listener, so unregister it after executing this plan
    planExecutor.registerListener(PLAN_LISTENER_ID,  opResult -> {
      if (opResult.getType().equals(Op.OpType.DEALLOCATE)) {
        deletedExecutorId.set(((DeallocateOp) opResult.getOp()).getExecutorId());
      }
    });
    planExecutor.execute(plan).get();

    planExecutor.unregisterListener(PLAN_LISTENER_ID);

    // check the result
    if (deletedExecutorId.get() == null) {
      throw new RuntimeException();
    }

    AllocatedExecutor deletedExecutor = null;
    for (final AllocatedExecutor associator : executors) {
      if (associator.getId().equals(deletedExecutorId.get())) {
        deletedExecutor = associator;
        break;
      }
    }

    if (deletedExecutor == null) {
      throw new RuntimeException();
    }

    try {
      etMaster.getExecutor(deletedExecutorId.get());
      throw new RuntimeException();
    } catch (ExecutorNotExistException e) {
      // expected exception
    }

    final Set<String> tableAssociatorIds = etMaster.getTable(tableId).getPartitionInfo().keySet();
    if (tableAssociatorIds.contains(deletedExecutorId.get())) {
      throw new RuntimeException();
    }

    LOG.log(Level.INFO, "runDelOneAssociatorTest is completed");

    return deletedExecutor;
  }
}
