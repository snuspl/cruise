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
import edu.snu.cay.services.em.utils.AvroUtils;
import edu.snu.cay.utils.trace.HTraceUtils;
import edu.snu.cay.utils.SingleMessageExtractor;
import org.apache.commons.lang.math.LongRange;
import org.apache.reef.annotations.audience.Private;
import org.htrace.Trace;
import org.htrace.TraceInfo;
import org.htrace.TraceScope;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.io.network.Message;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.HashSet;
import java.util.Set;
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

  private static final String ON_REGIS_MSG = "onRegisMsg";
  private static final String ON_DATA_ACK_MSG = "onDataAckMsg";
  private static final String ON_UPDATE_ACK_MSG = "onUpdateAckMsg";
  private static final String ON_OWNERSHIP_MSG = "onOwnershipMsg";
  private static final String ON_OWNERSHIP_ACK_MSG = "onOwnershipAckMsg";
  private static final String ON_FAILURE_MSG = "onFailureMsg";

  private final PartitionManager partitionManager;
  private final MigrationManager migrationManager;

  @Inject
  private ElasticMemoryMsgHandler(final PartitionManager partitionManager,
                                  final MigrationManager migrationManager) {
    this.partitionManager = partitionManager;
    this.migrationManager = migrationManager;
  }

  @Override
  public void onNext(final Message<AvroElasticMemoryMessage> msg) {
    LOG.entering(ElasticMemoryMsgHandler.class.getSimpleName(), "onNext", msg);

    final AvroElasticMemoryMessage innerMsg = SingleMessageExtractor.extract(msg);
    switch (innerMsg.getType()) {
    case RegisMsg:
      onRegisMsg(innerMsg);
      break;

    case DataAckMsg:
      onDataAckMsg(innerMsg);
      break;

    case UpdateAckMsg:
      onUpdateAckMsg(innerMsg);
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

  private void onRegisMsg(final AvroElasticMemoryMessage msg) {
    try (final TraceScope onRegisMsgScope = Trace.startSpan(ON_REGIS_MSG, HTraceUtils.fromAvro(msg.getTraceInfo()))) {

      final RegisMsg regisMsg = msg.getRegisMsg();

      // register a partition for the evaluator as specified in the message
      partitionManager.register(msg.getSrcId().toString(),
          regisMsg.getDataType().toString(), regisMsg.getIdRange().getMin(), regisMsg.getIdRange().getMax());
    }
  }

  private void onDataAckMsg(final AvroElasticMemoryMessage msg) {
    try (final TraceScope onDataAckMsgScope = Trace.startSpan(ON_DATA_ACK_MSG,
        HTraceUtils.fromAvro(msg.getTraceInfo()))) {

      final String operationId = msg.getOperationId().toString();

      // Add the range information to the corresponding Migration.
      final Set<LongRange> ranges = new HashSet<>();
      for (final AvroLongRange range : msg.getDataAckMsg().getIdRange()) {
        ranges.add(AvroUtils.fromAvroLongRange(range));
      }
      migrationManager.setMovedRanges(operationId, ranges);

      // Wait for the user's approval to update.
      // Once EM allows remote access to the data, we can remove this barrier letting EM update its state automatically.
      migrationManager.waitUpdate(operationId);
    }
  }

  private void onUpdateAckMsg(final AvroElasticMemoryMessage msg) {
    try (final TraceScope onUpdateAckMsgScope =
             Trace.startSpan(ON_UPDATE_ACK_MSG, HTraceUtils.fromAvro(msg.getTraceInfo()))) {
      final UpdateResult updateResult = msg.getUpdateAckMsg().getResult();
      final String operationId = msg.getOperationId().toString();

      switch (updateResult) {

      case RECEIVER_UPDATED:
        // After receiver updates its state, the partition is guaranteed to be accessible in the receiver.
        // So we can move the partition and update the sender.
        final TraceInfo traceInfo = TraceInfo.fromSpan(onUpdateAckMsgScope.getSpan());
        migrationManager.movePartition(operationId, traceInfo);
        migrationManager.updateSender(operationId, traceInfo);
        break;

      case SENDER_UPDATED:
        // Finish the migration notifying the result to the client via callback.
        migrationManager.finishMigration(operationId);
        break;

      default:
        throw new RuntimeException("Undefined result: " + updateResult);
      }
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
