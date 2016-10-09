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
import org.htrace.Trace;
import org.htrace.TraceScope;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.io.network.Message;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Evaluator-side message handler.
 * It handles control messages from the driver and data/ownership messages from
 * other evaluators.
 */
@EvaluatorSide
@Private
public final class ElasticMemoryMsgHandler implements EventHandler<Message<EMMsg>> {
  private static final Logger LOG = Logger.getLogger(ElasticMemoryMsgHandler.class.getName());

  private final OperationRouter router;
  private final RemoteOpHandler remoteOpHandler;
  private final MigrationExecutor migrationExecutor;

  @Inject
  private ElasticMemoryMsgHandler(final OperationRouter router,
                                  final RemoteOpHandler remoteOpHandler,
                                  final MigrationExecutor migrationExecutor) {
    this.router = router;
    this.remoteOpHandler = remoteOpHandler;
    this.migrationExecutor = migrationExecutor;
  }

  @Override
  public void onNext(final Message<EMMsg> msg) {
    LOG.entering(ElasticMemoryMsgHandler.class.getSimpleName(), "onNext", msg);

    final EMMsg innerMsg = SingleMessageExtractor.extract(msg);
    switch (innerMsg.getType()) {
    case RoutingTableMsg:
      onRoutingTableMsg(innerMsg.getRoutingTableMsg());
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

    LOG.exiting(ElasticMemoryMsgHandler.class.getSimpleName(), "onNext", msg);
  }

  private void onRoutingTableMsg(final RoutingTableMsg msg) {
    switch (msg.getType()) {
    case RoutingTableInitMsg:
      onRoutingTableInitMsg(msg);
      break;

    case RoutingTableUpdateMsg:
      onRoutingTableUpdateMsg(msg);
      break;

    default:
      throw new RuntimeException("Unexpected message: " + msg);
    }
  }

  private void onRoutingTableInitMsg(final RoutingTableMsg msg) {
    router.initRoutingTableWithDriver(msg.getRoutingTableInitMsg().getBlockLocations());
  }

  private void onRoutingTableUpdateMsg(final RoutingTableMsg msg) {
    Trace.setProcessId("eval");
    try (final TraceScope onRoutingTableUpdateMsgScope = Trace.startSpan("on_table_update_msg",
        HTraceUtils.fromAvro(msg.getTraceInfo()))) {
      final RoutingTableUpdateMsg routingTableUpdateMsg = msg.getRoutingTableUpdateMsg();

      final List<Integer> blockIds = routingTableUpdateMsg.getBlockIds();
      final int newOwnerId = getStoreId(routingTableUpdateMsg.getNewEvalId().toString());
      final int oldOwnerId = getStoreId(routingTableUpdateMsg.getOldEvalId().toString());

      LOG.log(Level.INFO, "Update routing table. [newOwner: {0}, oldOwner: {1}, blocks: {2}]",
          new Object[]{newOwnerId, oldOwnerId, blockIds});

      for (final int blockId : blockIds) {
        router.updateOwnership(blockId, oldOwnerId, newOwnerId);
      }
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
