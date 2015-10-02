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
import edu.snu.cay.services.em.driver.MigrationManager;
import edu.snu.cay.services.em.driver.api.EMResourceRequestManager;
import edu.snu.cay.services.em.driver.api.ElasticMemory;
import edu.snu.cay.services.em.msg.api.ElasticMemoryCallbackRouter;
import edu.snu.cay.utils.trace.HTrace;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.math.LongRange;
import org.apache.reef.driver.context.ActiveContext;
import org.htrace.Trace;
import org.htrace.TraceInfo;
import org.htrace.TraceScope;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.wake.EventHandler;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

@DriverSide
public final class ElasticMemoryImpl implements ElasticMemory {
  private static final String MOVE = "move";
  private static final String APPLY_UPDATES = "apply_updates";

  private final EvaluatorRequestor requestor;
  private final ElasticMemoryCallbackRouter callbackRouter;
  private final MigrationManager migrationManager;

  private final AtomicLong operationIdCounter = new AtomicLong();
  /**
   * EM resource request manager.
   */
  private final EMResourceRequestManager resourceRequestManager;

  @Inject
  private ElasticMemoryImpl(final EvaluatorRequestor requestor,
                            final ElasticMemoryCallbackRouter callbackRouter,
                            final MigrationManager migrationManager,
                            final EMResourceRequestManager resourceRequestManager,
                            final HTrace hTrace) {
    hTrace.initialize();
    this.requestor = requestor;
    this.callbackRouter = callbackRouter;
    this.migrationManager = migrationManager;
    this.resourceRequestManager = resourceRequestManager;
  }

  /**
   * Request for evaluators and remember passed callback.
   * Currently assumes that every request has same memory size and cores.
   * TODO #188: Support heterogeneous evaluator requests
   */
  @Override
  public void add(final int number, final int megaBytes, final int cores,
                  @Nullable final EventHandler<ActiveContext> callback) {
    for (int i = 0; i < number; i++) {
      resourceRequestManager.register(callback);
    }
    requestor.submit(EvaluatorRequest.newBuilder()
        .setNumber(number)
        .setMemory(megaBytes)
        .setNumberOfCores(cores)
        .build());
  }

  // TODO #112: implement delete
  @Override
  public void delete(final String evalId) {
    throw new NotImplementedException();
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
                   @Nullable final EventHandler<AvroElasticMemoryMessage> callback) {
    try (final TraceScope traceScope = Trace.startSpan(MOVE)) {
      final String operationId = MOVE + "-" + Long.toString(operationIdCounter.getAndIncrement());
      final TraceInfo traceInfo = TraceInfo.fromSpan(traceScope.getSpan());
      callbackRouter.register(operationId, callback);
      migrationManager.startMigration(operationId, srcEvalId, destEvalId, dataType, idRangeSet, traceInfo);
    }
  }

  @Override
  public void move(final String dataType,
                   final int numUnits,
                   final String srcEvalId,
                   final String destEvalId,
                   @Nullable final EventHandler<AvroElasticMemoryMessage> callback) {
    try (final TraceScope traceScope = Trace.startSpan(MOVE)) {
      final String operationId = MOVE + "-" + Long.toString(operationIdCounter.getAndIncrement());
      final TraceInfo traceInfo = TraceInfo.fromSpan(traceScope.getSpan());
      callbackRouter.register(operationId, callback);
      migrationManager.startMigration(operationId, srcEvalId, destEvalId, dataType, numUnits, traceInfo);
    }
  }

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
}
