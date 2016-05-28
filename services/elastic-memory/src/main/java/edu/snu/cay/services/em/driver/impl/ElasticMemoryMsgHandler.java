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
public final class ElasticMemoryMsgHandler implements EventHandler<Message<AvroElasticMemoryMessage>> {
  private static final Logger LOG = Logger.getLogger(ElasticMemoryMsgHandler.class.getName());

  private static final String ON_ROUTING_INIT_REQ_MSG = "onRoutingTableInitReqMsg";
  private static final String ON_OWNERSHIP_MSG = "onOwnershipMsg";
  private static final String ON_OWNERSHIP_ACK_MSG = "onOwnershipAckMsg";
  private static final String ON_FAILURE_MSG = "onFailureMsg";

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
  public void onNext(final Message<AvroElasticMemoryMessage> msg) {
    LOG.entering(ElasticMemoryMsgHandler.class.getSimpleName(), "onNext", msg);

    final AvroElasticMemoryMessage innerMsg = SingleMessageExtractor.extract(msg);
    switch (innerMsg.getType()) {
    case RoutingTableInitReqMsg:
      onRoutingTableInitReqMsg(innerMsg);
      break;

    case OwnershipMsg:
      onOwnershipMsg(innerMsg);
      break;

    case OwnershipAckMsg:
      onOwnershipAckMsg(innerMsg);
      break;

    case FailureMsg:
      onFailureMsg(innerMsg);
      break;

    default:
      throw new RuntimeException("Unexpected message: " + msg);
    }

    LOG.exiting(ElasticMemoryMsgHandler.class.getSimpleName(), "onNext", msg);
  }

  private void onRoutingTableInitReqMsg(final AvroElasticMemoryMessage msg) {
    try (final TraceScope onRoutingTableInitReqMsgScope = Trace.startSpan(ON_ROUTING_INIT_REQ_MSG,
        HTraceUtils.fromAvro(msg.getTraceInfo()))) {

      final List<Integer> blockLocations = blockManager.getBlockLocations();

      final TraceInfo traceInfo = TraceInfo.fromSpan(onRoutingTableInitReqMsgScope.getSpan());
      msgSender.get().sendRoutingTableInitMsg(msg.getSrcId().toString(), blockLocations, traceInfo);
    }
  }

  private void onOwnershipAckMsg(final AvroElasticMemoryMessage msg) {
    try (final TraceScope onOwnershipMsgScope = Trace.startSpan(ON_OWNERSHIP_ACK_MSG,
        HTraceUtils.fromAvro(msg.getTraceInfo()))) {

      final String operationId = msg.getOperationId().toString();
      final OwnershipAckMsg ownershipAckMsg = msg.getOwnershipAckMsg();

      final int blockId = ownershipAckMsg.getBlockId();
      migrationManager.markBlockAsMoved(operationId, blockId);
    }
  }

  private void onOwnershipMsg(final AvroElasticMemoryMessage msg) {
    try (final TraceScope onOwnershipMsgScope = Trace.startSpan(ON_OWNERSHIP_MSG,
        HTraceUtils.fromAvro(msg.getTraceInfo()))) {

      final String operationId = msg.getOperationId().toString();
      final int blockId = msg.getOwnershipMsg().getBlockId();
      final int oldOwnerId = msg.getOwnershipMsg().getOldOwnerId();
      final int newOwnerId = msg.getOwnershipMsg().getNewOwnerId();

      // Update the owner and send ownership message to the old Owner.
      final TraceInfo traceInfo = TraceInfo.fromSpan(onOwnershipMsgScope.getSpan());
      migrationManager.updateOwner(operationId, blockId, oldOwnerId, newOwnerId, traceInfo);
    }
  }

  private void onFailureMsg(final AvroElasticMemoryMessage msg) {
    try (final TraceScope onFailureMsgScope =
             Trace.startSpan(ON_FAILURE_MSG, HTraceUtils.fromAvro(msg.getTraceInfo()))) {
      final FailureMsg failureMsg = msg.getFailureMsg();

      final String operationId = failureMsg.getOperationId().toString();
      final String reason = failureMsg.getReason().toString();

      migrationManager.failMigration(operationId, reason);
    }
  }
}
