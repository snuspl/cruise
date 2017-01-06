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
package edu.snu.cay.services.em.evaluator.impl;

import edu.snu.cay.services.em.avro.*;
import edu.snu.cay.services.em.evaluator.api.MigrationExecutor;
import edu.snu.cay.services.em.evaluator.api.RemoteOpHandler;
import edu.snu.cay.utils.trace.HTraceUtils;
import edu.snu.cay.utils.SingleMessageExtractor;
import org.apache.reef.annotations.audience.Private;
import org.htrace.Span;
import org.htrace.Trace;
import org.htrace.TraceInfo;
import org.htrace.TraceScope;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.io.network.Message;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Evaluator-side message handler.
 * It handles control messages from the driver and data/ownership messages from
 * other evaluators.
 */
@EvaluatorSide
@Private
public final class EMMsgHandler implements EventHandler<Message<EMMsg>> {
  private static final Logger LOG = Logger.getLogger(EMMsgHandler.class.getName());
  private static final int NUM_OWNERSHIP_CACHE_UPDATE_MSG_RECEIVER_THREADS = 2;

  private final OwnershipCache ownershipCache;
  private final RemoteOpHandler remoteOpHandler;
  private final MigrationExecutor migrationExecutor;

  private final ExecutorService ownershipCacheUpdateMsgHandlerExecutor
      = Executors.newFixedThreadPool(NUM_OWNERSHIP_CACHE_UPDATE_MSG_RECEIVER_THREADS);

  @Inject
  private EMMsgHandler(final OwnershipCache ownershipCache,
                       final RemoteOpHandler remoteOpHandler,
                       final MigrationExecutor migrationExecutor) {
    this.ownershipCache = ownershipCache;
    this.remoteOpHandler = remoteOpHandler;
    this.migrationExecutor = migrationExecutor;
  }

  @Override
  public void onNext(final Message<EMMsg> msg) {
    LOG.entering(EMMsgHandler.class.getSimpleName(), "onNext", msg);

    final EMMsg innerMsg = SingleMessageExtractor.extract(msg);
    switch (innerMsg.getType()) {
    case OwnershipCacheMsg:
      onOwnershipCacheMsg(innerMsg.getOwnershipCacheMsg());
      break;

    case RemoteOpMsg:
      onRemoteOpMsg(innerMsg.getRemoteOpMsg());
      break;

    case MigrationMsg:
      onMigrationMsg(innerMsg.getMigrationMsg());
      break;

    default:
      throw new RuntimeException("Unexpected message: " + msg);
    }

    LOG.exiting(EMMsgHandler.class.getSimpleName(), "onNext", msg);
  }

  private void onOwnershipCacheMsg(final OwnershipCacheMsg msg) {
    switch (msg.getType()) {
    case OwnershipCacheInitMsg:
      onOwnershipCacheInitMsg(msg);
      break;

    case OwnershipCacheUpdateMsg:
      onOwnershipCacheUpdateMsg(msg);
      break;

    default:
      throw new RuntimeException("Unexpected message: " + msg);
    }
  }

  private void onOwnershipCacheInitMsg(final OwnershipCacheMsg msg) {
    ownershipCache.initOwnershipInfo(msg.getOwnershipCacheInitMsg().getBlockLocations());
  }

  private void onOwnershipCacheUpdateMsg(final OwnershipCacheMsg msg) {
    // We should detach the span when we transit to another thread (local or remote),
    // and the detached span should call Trace.continueSpan(detached).close() explicitly
    // for stitching the spans from other threads as its children
    Span detached = null;

    Trace.setProcessId("eval");
    try (TraceScope onOwnershipCacheUpdateMsgScope = Trace.startSpan("on_ownership_update_msg",
        HTraceUtils.fromAvro(msg.getTraceInfo()))) {

      detached = onOwnershipCacheUpdateMsgScope.detach();
      final TraceInfo traceInfo = TraceInfo.fromSpan(detached);

      ownershipCacheUpdateMsgHandlerExecutor.submit(new Runnable() {
        @Override
        public void run() {
          final OwnershipCacheUpdateMsg ownershipCacheUpdateMsg = msg.getOwnershipCacheUpdateMsg();

          final List<Integer> blockIds = ownershipCacheUpdateMsg.getBlockIds();
          final int newOwnerId = getStoreId(ownershipCacheUpdateMsg.getNewEvalId().toString());
          final int oldOwnerId = getStoreId(ownershipCacheUpdateMsg.getOldEvalId().toString());

          LOG.log(Level.INFO, "Update ownership cache. [newOwner: {0}, oldOwner: {1}, blocks: {2}]",
              new Object[]{newOwnerId, oldOwnerId, blockIds});

          for (final int blockId : blockIds) {
            ownershipCache.updateOwnership(blockId, oldOwnerId, newOwnerId);
          }
        }
      });
    }
  }

  /**
   * Passes the request and result msgs of remote op to {@link RemoteOpHandler}.
   */
  private void onRemoteOpMsg(final RemoteOpMsg msg) {
    remoteOpHandler.onNext(msg);
  }

  /**
   * Passes the msg of migration to {@link MigrationExecutor}.
   */
  private void onMigrationMsg(final MigrationMsg msg) {
    migrationExecutor.onNext(msg);
  }

  /**
   * Converts evaluator id to store id.
   */
  private int getStoreId(final String evalId) {
    return Integer.valueOf(evalId.split("-")[1]);
  }
}
