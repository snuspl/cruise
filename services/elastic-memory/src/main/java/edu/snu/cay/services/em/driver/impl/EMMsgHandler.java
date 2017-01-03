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
import edu.snu.cay.services.em.msg.api.EMMsgSender;
import edu.snu.cay.utils.trace.HTraceUtils;
import edu.snu.cay.utils.SingleMessageExtractor;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.util.Optional;
import org.htrace.Span;
import org.htrace.Trace;
import org.htrace.TraceInfo;
import org.htrace.TraceScope;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.io.network.Message;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver-side message handler.
 * It handles migration related messages from evaluators.
 */
@DriverSide
@Private
public final class EMMsgHandler implements EventHandler<Message<EMMsg>> {
  private static final Logger LOG = Logger.getLogger(EMMsgHandler.class.getName());

  private final BlockManager blockManager;
  private final MigrationManager migrationManager;

  private final InjectionFuture<EMMsgSender> msgSender;

  /**
   * In ownership-first migration, the {@link OwnershipAckMsg} and {@link BlockMovedMsg} can arrive out-of-order.
   * Using this map, we can verify the correct order of them (OwnershipAckMsg -> BlockMovedMsg) in message handlers.
   * A later message will call {@link #processBlockMovedMsg} to put a received block into MemoryStore.
   * A value is {@link Optional#empty()} when an entry is put by OwnershipAckMsg.
   *
   * In data-first migration, on the other hand, {@link OwnershipMsg} always precedes {@link BlockMovedMsg}.
   * So {@link #onOwnershipMsg(MigrationMsg)} simply marks that OwnershipMsg has arrived,
   * and {@link #onBlockMovedMsg(MigrationMsg)} wraps up the migration without any concern.
   */
  private final Map<Integer, Optional<TraceInfo>> msgArrivedBlocks = new ConcurrentHashMap<>();

  @Inject
  private EMMsgHandler(final BlockManager blockManager,
                       final MigrationManager migrationManager,
                       final InjectionFuture<EMMsgSender> msgSender) {
    this.blockManager = blockManager;
    this.migrationManager = migrationManager;
    this.msgSender = msgSender;
  }

  @Override
  public void onNext(final Message<EMMsg> msg) {
    LOG.entering(EMMsgHandler.class.getSimpleName(), "onNext", msg);

    Trace.setProcessId("driver");
    final EMMsg innerMsg = SingleMessageExtractor.extract(msg);
    switch (innerMsg.getType()) {
    case RoutingTableMsg:
      onRoutingTableMsg(innerMsg.getRoutingTableMsg());
      break;

    case MigrationMsg:
      onMigrationMsg(innerMsg.getMigrationMsg());
      break;

    default:
      throw new RuntimeException("Unexpected message: " + msg);
    }

    LOG.exiting(EMMsgHandler.class.getSimpleName(), "onNext", msg);
  }

  private void onRoutingTableMsg(final RoutingTableMsg msg) {
    switch (msg.getType()) {
    case RoutingTableInitReqMsg:
      onRoutingTableInitReqMsg(msg);
      break;

    default:
      throw new RuntimeException("Unexpected message: " + msg);
    }
  }

  private void onRoutingTableInitReqMsg(final RoutingTableMsg msg) {
    try (TraceScope onRoutingTableInitReqMsgScope = Trace.startSpan("on_routing_table_init_req_msg",
        HTraceUtils.fromAvro(msg.getTraceInfo()))) {

      final RoutingTableInitReqMsg routingTableInitReqMsg = msg.getRoutingTableInitReqMsg();
      final String evalId = routingTableInitReqMsg.getEvalId().toString();
      final List<Integer> blockLocations = blockManager.getBlockLocations();

      msgSender.get().sendRoutingTableInitMsg(evalId, blockLocations,
          TraceInfo.fromSpan(onRoutingTableInitReqMsgScope.getSpan()));
    }
  }

  private void onMigrationMsg(final MigrationMsg msg) {
    switch (msg.getType()) {
    case OwnershipMsg:
      onOwnershipMsg(msg);
      break;

    case OwnershipAckMsg:
      onOwnershipAckMsg(msg);
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

  /**
   * (Data-first migration only) Handles the OwnershipMsg from the receiver
   * that reports an update of a block's ownership.
   */
  private void onOwnershipMsg(final MigrationMsg msg) {
    final String operationId = msg.getOperationId().toString();
    final OwnershipMsg ownershipMsg = msg.getOwnershipMsg();
    final int blockId = ownershipMsg.getBlockId();
    final String senderId = ownershipMsg.getSenderId().toString();
    final int oldOwnerId = ownershipMsg.getOldOwnerId();
    final int newOwnerId = ownershipMsg.getNewOwnerId();

    try (TraceScope onOwnershipMsgScope = Trace.startSpan(
        String.format("on_ownership_msg. blockId: %d", blockId),
        HTraceUtils.fromAvro(msg.getTraceInfo()))) {

      // In data-first migration, OwnershipMsg always precedes BlockMovedMsg.
      // So we don't need synchronization here, and we can simply mark that OwnershipMsg has arrived allowing
      // onBlockMoveMsg to process BlockMovedMsg later.
      msgArrivedBlocks.put(blockId, Optional.empty());

      // Update the owner of the block to the new one.
      migrationManager.updateOwner(blockId, oldOwnerId, newOwnerId);

      // Send the OwnershipMessage to update the owner in the sender memoryStore
      msgSender.get().sendOwnershipMsg(Optional.of(senderId), senderId, operationId, blockId, oldOwnerId, newOwnerId,
          TraceInfo.fromSpan(onOwnershipMsgScope.getSpan()));
    }
  }

  /**
   * (Ownership-first migration only) Handles the OwnershipAckMsg from the sender
   * that reports an update of a block's ownership.
   */
  private void onOwnershipAckMsg(final MigrationMsg msg) {
    final String operationId = msg.getOperationId().toString();
    final OwnershipAckMsg ownershipAckMsg = msg.getOwnershipAckMsg();
    final int blockId = ownershipAckMsg.getBlockId();
    final int oldOwnerId = ownershipAckMsg.getOldOwnerId();
    final int newOwnerId = ownershipAckMsg.getNewOwnerId();

    try (TraceScope onOwnershipAckMsgScope = Trace.startSpan(
        String.format("on_ownership_ack_msg. blockId: %d", blockId),
        HTraceUtils.fromAvro(msg.getTraceInfo()))) {

      // Update the owner of the block to the new one.
      migrationManager.updateOwner(blockId, oldOwnerId, newOwnerId);

      // In ownership-first migration, the order of OwnershipAckMsg and BlockMovedMsg is not fixed.
      // However, BlockMovedMsg should be handled after updating ownership by OwnershipAckMsg.
      // So if BlockMovedMsg for the same block has been already arrived, handle that msg now.
      final boolean ownershipAckMsgArrivedFirst; // If true, the messages have arrived in order
      TraceInfo blockMovedMsgTraceInfo = null;
      synchronized (msgArrivedBlocks) {
        ownershipAckMsgArrivedFirst = !msgArrivedBlocks.containsKey(blockId);
        if (ownershipAckMsgArrivedFirst) {
          msgArrivedBlocks.put(blockId, Optional.empty());
        } else {
          blockMovedMsgTraceInfo = msgArrivedBlocks.remove(blockId).get();
        }
      }

      // If the messages have arrived out of order, there should be an awaiting blockMovedMsg to be processed.
      if (!ownershipAckMsgArrivedFirst) {
        processBlockMovedMsg(operationId, blockId, blockMovedMsgTraceInfo);
      }
    }
  }

  /**
   * Handles BlockMovedMsg from the sender that reports data migration for a block has been finished successfully.
   */
  private void onBlockMovedMsg(final MigrationMsg msg) {
    final String operationId = msg.getOperationId().toString();
    final BlockMovedMsg blockMovedMsg = msg.getBlockMovedMsg();
    final int blockId = blockMovedMsg.getBlockId();

    // We should detach the span when we transit to another thread (local or remote),
    // and the detached span should call Trace.continueSpan(detached).close() explicitly
    // for stitching the spans from other threads as its children
    Span detached = null;

    try (TraceScope onBlockMovedMsgScope = Trace.startSpan(
        String.format("on_block_moved_msg. blockId: %d", blockId), HTraceUtils.fromAvro(msg.getTraceInfo()))) {

      // In ownership-first migration, the order of OwnershipAckMsg and BlockMovedMsg is not fixed.
      // However, BlockMovedMsg should be handled after updating ownership by OwnershipAckMsg.
      // So handle BlockMovedMsg now, if OwnershipAckMsg for the same block has been already arrived.
      // Otherwise handle it in future when corresponding OwnershipAckMsg arrives
      final boolean ownershipMsgArrivedFirst; // If true, the messages have arrived in order
      synchronized (msgArrivedBlocks) {
        ownershipMsgArrivedFirst = msgArrivedBlocks.containsKey(blockId);
        if (ownershipMsgArrivedFirst) {
          msgArrivedBlocks.remove(blockId);
        } else {
          detached = onBlockMovedMsgScope.detach();
          // traceInfo is null, if onBlockMovedMsgScope is NullScope
          msgArrivedBlocks.put(blockId, Optional.ofNullable(TraceInfo.fromSpan(detached)));
        }
      }

      // If the messages have arrived in order, blockMovedMsg should be processed here.
      if (ownershipMsgArrivedFirst) {
        processBlockMovedMsg(operationId, blockId, TraceInfo.fromSpan(onBlockMovedMsgScope.getSpan()));
      }
    } finally {
      Trace.continueSpan(detached).close();
    }
  }

  private void processBlockMovedMsg(final String operationId, final int blockId, final TraceInfo traceInfo) {
    LOG.log(Level.INFO, "mark block as moved. blockId: {0}", blockId);
    migrationManager.markBlockAsMoved(operationId, blockId, traceInfo);
  }

  private void onFailureMsg(final MigrationMsg msg) {
    try (TraceScope onFailureMsgScope =
             Trace.startSpan("on_failure_msg", HTraceUtils.fromAvro(msg.getTraceInfo()))) {
      final FailureMsg failureMsg = msg.getFailureMsg();

      final String operationId = failureMsg.getOperationId().toString();
      final String reason = failureMsg.getReason().toString();

      migrationManager.failMigration(operationId, reason);
    }
  }
}
