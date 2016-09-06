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

  private static final String ON_ROUTING_INIT_REQ_MSG = "on_routing_table_init_req_msg";
  private static final String ON_OWNERSHIP_MSG = "on_ownership_msg";
  private static final String ON_OWNERSHIP_ACK_MSG = "on_ownership_ack_msg";
  private static final String ON_FAILURE_MSG = "on_failure_msg";

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
    Trace.setProcessId("driver");
    try (final TraceScope onRoutingTableInitReqMsgScope = Trace.startSpan(ON_ROUTING_INIT_REQ_MSG,
        HTraceUtils.fromAvro(msg.getTraceInfo()))) {

      final List<Integer> blockLocations = blockManager.getBlockLocations();

      msgSender.get().sendRoutingTableInitMsg(msg.getSrcId().toString(), blockLocations,
          TraceInfo.fromSpan(onRoutingTableInitReqMsgScope.getSpan()));
    }
  }

  private void onOwnershipAckMsg(final AvroElasticMemoryMessage msg) {
    Trace.setProcessId("driver");
    try (final TraceScope onOwnershipAckMsgScope = Trace.startSpan("[5]" + ON_OWNERSHIP_ACK_MSG,
        HTraceUtils.fromAvro(msg.getTraceInfo()))) {

      final String operationId = msg.getOperationId().toString();
      final OwnershipAckMsg ownershipAckMsg = msg.getOwnershipAckMsg();
      final int blockId = ownershipAckMsg.getBlockId();

      migrationManager.markBlockAsMoved(operationId, blockId, TraceInfo.fromSpan(onOwnershipAckMsgScope.getSpan()));
    }
  }

  private void onOwnershipMsg(final AvroElasticMemoryMessage msg) {
    Trace.setProcessId("driver");
    try (final TraceScope onOwnershipMsgScope = Trace.startSpan("[3]" + ON_OWNERSHIP_MSG,
        HTraceUtils.fromAvro(msg.getTraceInfo()))) {

      final String operationId = msg.getOperationId().toString();
      final int blockId = msg.getOwnershipMsg().getBlockId();
      final int oldOwnerId = msg.getOwnershipMsg().getOldOwnerId();
      final int newOwnerId = msg.getOwnershipMsg().getNewOwnerId();

      // Update the owner and send ownership message to the old Owner.
      migrationManager.updateOwner(operationId, blockId, oldOwnerId, newOwnerId,
          TraceInfo.fromSpan(onOwnershipMsgScope.getSpan()));
    }
  }

  private void onFailureMsg(final AvroElasticMemoryMessage msg) {
    Trace.setProcessId("driver");
    try (final TraceScope onFailureMsgScope =
             Trace.startSpan(ON_FAILURE_MSG, HTraceUtils.fromAvro(msg.getTraceInfo()))) {
      final FailureMsg failureMsg = msg.getFailureMsg();

      final String operationId = failureMsg.getOperationId().toString();
      final String reason = failureMsg.getReason().toString();

      migrationManager.failMigration(operationId, reason);
    }
  }
}
