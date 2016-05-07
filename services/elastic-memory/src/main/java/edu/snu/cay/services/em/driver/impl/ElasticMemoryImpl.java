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
package edu.snu.cay.services.em.driver.impl;

import edu.snu.cay.services.em.avro.AvroElasticMemoryMessage;
import edu.snu.cay.services.em.avro.Result;
import edu.snu.cay.services.em.avro.ResultMsg;
import edu.snu.cay.services.em.avro.Type;
import edu.snu.cay.services.em.driver.api.EMDeleteExecutor;
import edu.snu.cay.services.em.driver.api.ElasticMemory;
import edu.snu.cay.services.em.optimizer.api.EvaluatorParameters;
import edu.snu.cay.services.evalmanager.api.EvaluatorManager;
import edu.snu.cay.utils.trace.HTrace;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.math.LongRange;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.tang.InjectionFuture;
import org.htrace.Trace;
import org.htrace.TraceInfo;
import org.htrace.TraceScope;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.wake.EventHandler;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

@DriverSide
@Private
public final class ElasticMemoryImpl implements ElasticMemory {
  private static final Logger LOG = Logger.getLogger(ElasticMemoryImpl.class.getName());
  private static final String MOVE = "move";
  private static final String APPLY_UPDATES = "apply_updates";

  private final MigrationManager migrationManager;

  private final AtomicLong operationIdCounter = new AtomicLong();

  /**
   * Evaluator Manager, a unified path for requesting evaluators.
   * Helps managing REEF events related to evaluator and context.
   */
  private final EvaluatorManager evaluatorManager;

  private final InjectionFuture<EMDeleteExecutor> deleteExecutor;
  private final PartitionManager partitionManager;

  @Inject
  private ElasticMemoryImpl(final EvaluatorManager evaluatorManager,
                            final MigrationManager migrationManager,
                            final InjectionFuture<EMDeleteExecutor> deleteExecutor,
                            final PartitionManager partitionManager,
                            final HTrace hTrace) {
    hTrace.initialize();
    this.evaluatorManager = evaluatorManager;
    this.migrationManager = migrationManager;
    this.deleteExecutor = deleteExecutor;
    this.partitionManager = partitionManager;
  }

  /**
   * Request for evaluators and remember passed callback.
   * Currently assumes that every request has same memory size and cores.
   * TODO #188: Support heterogeneous evaluator requests
   */
  @Override
  public void add(final int number, final int megaBytes, final int cores,
                  final EventHandler<AllocatedEvaluator> evaluatorAllocatedHandler,
                  final List<EventHandler<ActiveContext>> contextActiveHandlerList) {
    if (number <= 0) {
      LOG.log(Level.WARNING, "Ignore an invalid request for {0} evaluators", number);
      return;
    }
    evaluatorManager.allocateEvaluators(number, evaluatorAllocatedHandler, contextActiveHandlerList);
  }

  /**
   * Removes partitions registered to deleting evalId.
   * After that, EMDeleteExecutor handles the actual deleting request.
   * TODO #205: Reconsider using of Avro message in EM's callback
   */
  @Override
  public void delete(final String evalId, @Nullable final EventHandler<AvroElasticMemoryMessage> callback) {
    final Set<String> dataTypeSet = partitionManager.getDataTypes(evalId);
    for (final String dataType : dataTypeSet) {
      final Set<LongRange> rangeSet = partitionManager.getRangeSet(evalId, dataType);
      // Deletion fails when the evaluator has remaining data
      if (!rangeSet.isEmpty()) {
        if (callback != null) {
          final AvroElasticMemoryMessage msg = AvroElasticMemoryMessage.newBuilder()
              .setType(Type.ResultMsg)
              .setResultMsg(ResultMsg.newBuilder().setResult(Result.FAILURE).build())
              .setSrcId(evalId)
              .setDestId("")
              .build();
          callback.onNext(msg);
        }
        return;
      }
    }

    if (callback == null) {
      deleteExecutor.get().execute(evalId, new EventHandler<AvroElasticMemoryMessage>() {
        @Override
        public void onNext(final AvroElasticMemoryMessage msg) {

        }
      });
    } else {
      deleteExecutor.get().execute(evalId, callback);
    }
  }

  // TODO #113: implement resize
  @Override
  public void resize(final String evalId, final int megaBytes, final int cores) {
    throw new NotImplementedException();
  }

  @Override
  public void move(final String dataType,
                   final Set<LongRange> idRangeSet,
                   final String srcEvalId,
                   final String destEvalId,
                   @Nullable final EventHandler<AvroElasticMemoryMessage> transferredCallback,
                   @Nullable final EventHandler<AvroElasticMemoryMessage> finishedCallback) {
    try (final TraceScope traceScope = Trace.startSpan(MOVE)) {
      final String operationId = MOVE + "-" + Long.toString(operationIdCounter.getAndIncrement());
      final TraceInfo traceInfo = TraceInfo.fromSpan(traceScope.getSpan());
      migrationManager.startMigration(operationId, srcEvalId, destEvalId, dataType, idRangeSet, traceInfo,
          transferredCallback, finishedCallback);
    }
  }

  @Override
  public void move(final String dataType,
                   final int numUnits,
                   final String srcEvalId,
                   final String destEvalId,
                   @Nullable final EventHandler<AvroElasticMemoryMessage> transferredCallback,
                   @Nullable final EventHandler<AvroElasticMemoryMessage> finishedCallback) {
    try (final TraceScope traceScope = Trace.startSpan(MOVE)) {
      final String operationId = MOVE + "-" + Long.toString(operationIdCounter.getAndIncrement());
      final TraceInfo traceInfo = TraceInfo.fromSpan(traceScope.getSpan());
      migrationManager.startMigration(operationId, srcEvalId, destEvalId, dataType, numUnits, traceInfo,
          transferredCallback, finishedCallback);
    }
  }

  @Override
  public void move(final String dataType, final int numBlocks, final String srcEvalId, final String destEvalId,
                   @Nullable final EventHandler<AvroElasticMemoryMessage> finishedCallback) {
    try (final TraceScope traceScope = Trace.startSpan(MOVE)) {
      final TraceInfo traceInfo = TraceInfo.fromSpan(traceScope.getSpan());
      final String operationId = MOVE + "-" + Long.toString(operationIdCounter.getAndIncrement());
      migrationManager.startMigration(operationId, srcEvalId, destEvalId, dataType, numBlocks, traceInfo,
          finishedCallback);
    }
  }

  /**
   * Apply the updates in the Driver and Evaluators' status.
   * It is synchronized to restrict at most one thread call this method at one time
   */
  @Override
  public synchronized void applyUpdates() {
    try (final TraceScope traceScope = Trace.startSpan(APPLY_UPDATES)) {
      final TraceInfo traceInfo = TraceInfo.fromSpan(traceScope.getSpan());
      migrationManager.applyUpdates(traceInfo);
    }
  }

  // TODO #114: implement checkpoint
  @Override
  public void checkpoint(final String evalId) {
    throw new NotImplementedException();
  }

  @Override
  public Map<Integer, Set<Integer>> getStoreIdToBlockIds() {
    return partitionManager.getStoreIdToBlockIds();
  }

  @Override
  public int getNumTotalBlocks() {
    return partitionManager.getNumTotalBlocks();
  }

  @Override
  public Map<String, EvaluatorParameters> generateEvalParams(final String dataType) {
    return partitionManager.generateEvalParams(dataType);
  }
}
