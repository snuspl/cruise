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

import edu.snu.cay.services.em.avro.*;
import edu.snu.cay.services.em.msg.api.ElasticMemoryMsgSender;
import edu.snu.cay.utils.trace.HTraceUtils;
import edu.snu.cay.utils.SingleMessageExtractor;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.tang.InjectionFuture;
import org.htrace.Trace;
import org.htrace.TraceInfo;
import org.htrace.TraceScope;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.io.network.Message;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Logger;

/**
 * Driver-side message handler.
 * Currently does nothing, but we need this class as a placeholder to
 * instantiate NetworkService.
 */
@DriverSide
@Private
public final class ElasticMemoryMsgHandler implements EventHandler<Message<EMMsg>> {
  private static final Logger LOG = Logger.getLogger(ElasticMemoryMsgHandler.class.getName());

  private final BlockManager blockManager;
  private final MigrationManager migrationManager;

  private final InjectionFuture<ElasticMemoryMsgSender> msgSender;

  @Inject
  private ElasticMemoryMsgHandler(final BlockManager blockManager,
                                  final MigrationManager migrationManager,
                                  final InjectionFuture<ElasticMemoryMsgSender> msgSender) {
    this.blockManager = blockManager;
    this.migrationManager = migrationManager;
    this.msgSender = msgSender;
  }

  @Override
  public void onNext(final Message<EMMsg> msg) {
    LOG.entering(ElasticMemoryMsgHandler.class.getSimpleName(), "onNext", msg);

    Trace.setProcessId("driver");
    final EMMsg innerMsg = SingleMessageExtractor.extract(msg);
    switch (innerMsg.getType()) {
    case EMRoutingTableMsg:
      onRoutingTableMsg(innerMsg.getEmRoutingTableMsg());
      break;

    case EMMigrationMsg:
      onMigrationMsg(innerMsg.getEmMigrationMsg());
      break;

    default:
      throw new RuntimeException("Unexpected message: " + msg);
    }

    LOG.exiting(ElasticMemoryMsgHandler.class.getSimpleName(), "onNext", msg);
  }

  private void onRoutingTableMsg(final EMRoutingTableMsg msg) {
    switch (msg.getType()) {
    case RoutingTableInitReqMsg:
      onRoutingTableInitReqMsg(msg);
      break;

    default:
      throw new RuntimeException("Unexpected message: " + msg);
    }
  }

  private void onRoutingTableInitReqMsg(final EMRoutingTableMsg msg) {
    try (final TraceScope onRoutingTableInitReqMsgScope = Trace.startSpan("on_routing_table_init_req_msg",
        HTraceUtils.fromAvro(msg.getTraceInfo()))) {

      final RoutingTableInitReqMsg routingTableInitReqMsg = msg.getRoutingTableInitReqMsg();
      final String evalId = routingTableInitReqMsg.getEvalId().toString();
      final List<Integer> blockLocations = blockManager.getBlockLocations();

      msgSender.get().sendRoutingTableInitMsg(evalId, blockLocations,
          TraceInfo.fromSpan(onRoutingTableInitReqMsgScope.getSpan()));
    }
  }

  private void onMigrationMsg(final EMMigrationMsg msg) {
    switch (msg.getType()) {
    case OwnershipMsg:
      onOwnershipMsg(msg);
      break;

    case BlockMovedMsg:
      onBlockMovedMsg(msg);
      break;

    case FailureMsg:
      onFailureMsg(msg);
      break;

    default:
      throw new RuntimeException("Unexpected message: " + msg);
    }
  }

  private void onBlockMovedMsg(final EMMigrationMsg msg) {
    final String operationId = msg.getOperationId().toString();
    final BlockMovedMsg blockMovedMsg = msg.getBlockMovedMsg();
    final int blockId = blockMovedMsg.getBlockId();

    try (final TraceScope onBlockMovedMsgScope = Trace.startSpan(
        String.format("on_block_moved_msg. blockId: %d", blockId), HTraceUtils.fromAvro(msg.getTraceInfo()))) {

      migrationManager.markBlockAsMoved(operationId, blockId, TraceInfo.fromSpan(onBlockMovedMsgScope.getSpan()));
    }
  }

  private void onOwnershipMsg(final EMMigrationMsg msg) {
    final String operationId = msg.getOperationId().toString();
    final int blockId = msg.getOwnershipMsg().getBlockId();
    final int oldOwnerId = msg.getOwnershipMsg().getOldOwnerId();
    final int newOwnerId = msg.getOwnershipMsg().getNewOwnerId();

    try (final TraceScope onOwnershipMsgScope = Trace.startSpan(
        String.format("on_ownership_msg. blockId: %d", blockId),
        HTraceUtils.fromAvro(msg.getTraceInfo()))) {

      // Update the owner and send ownership message to the old Owner.
      migrationManager.updateOwner(operationId, blockId, oldOwnerId, newOwnerId,
          TraceInfo.fromSpan(onOwnershipMsgScope.getSpan()));
    }
  }

  private void onFailureMsg(final EMMigrationMsg msg) {
    try (final TraceScope onFailureMsgScope =
             Trace.startSpan("on_failure_msg", HTraceUtils.fromAvro(msg.getTraceInfo()))) {
      final FailureMsg failureMsg = msg.getFailureMsg();

      final String operationId = failureMsg.getOperationId().toString();
      final String reason = failureMsg.getReason().toString();

      migrationManager.failMigration(operationId, reason);
    }
  }
}
