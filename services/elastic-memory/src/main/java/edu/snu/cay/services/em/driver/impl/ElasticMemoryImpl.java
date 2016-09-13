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
import edu.snu.cay.services.em.common.parameters.EMTraceEnabled;
import edu.snu.cay.services.em.driver.api.EMDeleteExecutor;
import edu.snu.cay.services.em.driver.api.EMRoutingTableUpdate;
import edu.snu.cay.services.em.driver.api.ElasticMemory;
import edu.snu.cay.services.evalmanager.api.EvaluatorManager;
import edu.snu.cay.utils.trace.HTrace;
import org.apache.commons.lang.NotImplementedException;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.htrace.Sampler;
import org.htrace.Trace;
import org.htrace.TraceInfo;
import org.htrace.TraceScope;

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
  private static final String OP_MOVE = "move";

  private final MigrationManager migrationManager;

  private final AtomicLong operationIdCounter = new AtomicLong();

  /**
   * Evaluator Manager, a unified path for requesting evaluators.
   * Helps managing REEF events related to evaluator and context.
   */
  private final EvaluatorManager evaluatorManager;

  private final InjectionFuture<EMDeleteExecutor> deleteExecutor;
  private final BlockManager blockManager;

  /**
   * Used to enable/disable trace spans.
   */
  private final Sampler traceSampler;

  @Inject
  private ElasticMemoryImpl(final EvaluatorManager evaluatorManager,
                            final MigrationManager migrationManager,
                            final InjectionFuture<EMDeleteExecutor> deleteExecutor,
                            final BlockManager blockManager,
                            @Parameter(EMTraceEnabled.class) final boolean emTraceEnabled,
                            final HTrace hTrace) {
    this.evaluatorManager = evaluatorManager;
    this.migrationManager = migrationManager;
    this.deleteExecutor = deleteExecutor;
    this.blockManager = blockManager;

    if (emTraceEnabled) {
      hTrace.initialize();
      traceSampler = Sampler.ALWAYS;
    } else {
      traceSampler = Sampler.NEVER;
    }
  }

  /**
   * Request for evaluators and remember passed callback.
   * Currently assumes that every request has same memory size and cores.
   * Note that the requests are handled only when the arguments are positive.
   * TODO #188: Support heterogeneous evaluator requests
   */
  @Override
  public void add(final int number, final int megaBytes, final int cores,
                  final EventHandler<AllocatedEvaluator> evaluatorAllocatedHandler,
                  final List<EventHandler<ActiveContext>> contextActiveHandlerList) {
    if (number == 0) {
      LOG.log(Level.WARNING, "Ignore the request for zero evaluator");
    } else if (number < 0) {
      throw new RuntimeException("The number of evaluators must be positive, but requested: " + number);
    } else if (megaBytes <= 0) {
      throw new RuntimeException("The capacity of evaluators must be positive, but requested: " + megaBytes);
    } else if (cores <= 0) {
      throw new RuntimeException("The CPU cores of evaluators must be positive, but requested: " + cores);
    } else {
      evaluatorManager.allocateEvaluators(number, evaluatorAllocatedHandler, contextActiveHandlerList);
    }
  }

  /**
   * Deletes an evaluator using EMDeleteExecutor.
   * The evaluator should have no blocks.
   * TODO #205: Reconsider using of Avro message in EM's callback
   */
  @Override
  public void delete(final String evalId, @Nullable final EventHandler<AvroElasticMemoryMessage> callback) {
    // Deletion fails when the evaluator has remaining data
    if (blockManager.getNumBlocks(evalId) > 0) {
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

    final boolean isSuccess;

    if (callback == null) {
      isSuccess = deleteExecutor.get().execute(evalId, new EventHandler<AvroElasticMemoryMessage>() {
        @Override
        public void onNext(final AvroElasticMemoryMessage msg) {

        }
      });
    } else {
      isSuccess = deleteExecutor.get().execute(evalId, callback);
    }

    if (isSuccess) {
      blockManager.deregisterEvaluator(evalId);
    }
  }

  // TODO #113: implement resize
  @Override
  public void resize(final String evalId, final int megaBytes, final int cores) {
    throw new NotImplementedException();
  }

  @Override
  public void move(final int numBlocks, final String srcEvalId, final String destEvalId,
                   @Nullable final EventHandler<AvroElasticMemoryMessage> finishedCallback) {
    Trace.setProcessId("elastic_memory");
    try (final TraceScope moveScope = Trace.startSpan(OP_MOVE, traceSampler)) {
      final String operationId = String.format("%s-%d", OP_MOVE, operationIdCounter.getAndIncrement());
      migrationManager.startMigration(operationId, srcEvalId, destEvalId, numBlocks,
          TraceInfo.fromSpan(moveScope.getSpan()), finishedCallback);
    }
  }

  // TODO #114: implement checkpoint
  @Override
  public void checkpoint(final String evalId) {
    throw new NotImplementedException();
  }

  @Override
  public void registerRoutingTableUpdateCallback(final String clientId,
                                                 final EventHandler<EMRoutingTableUpdate> updateCallback) {
    migrationManager.registerRoutingTableUpdateCallback(clientId, updateCallback);
  }

  @Override
  public void deregisterRoutingTableUpdateCallback(final String clientId) {
    migrationManager.deregisterRoutingTableUpdateCallback(clientId);
  }

  @Override
  public Map<Integer, Set<Integer>> getStoreIdToBlockIds() {
    return blockManager.getStoreIdToBlockIds();
  }

  @Override
  public Map<String, Integer> getEvalIdToNumBlocks() {
    return blockManager.getEvalIdToNumBlocks();
  }
}
