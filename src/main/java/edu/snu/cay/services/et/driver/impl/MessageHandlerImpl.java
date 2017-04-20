/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.cay.services.et.driver.impl;

import edu.snu.cay.services.et.avro.*;
import edu.snu.cay.services.et.common.api.MessageHandler;
import edu.snu.cay.services.et.driver.api.MetricReceiver;
import edu.snu.cay.utils.SingleMessageExtractor;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.io.network.Message;
import org.apache.reef.tang.InjectionFuture;

import javax.inject.Inject;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * A message handler implementation.
 */
@DriverSide
public final class MessageHandlerImpl implements MessageHandler {
  private final InjectionFuture<TableControlAgent> tableInitializerFuture;
  private final InjectionFuture<MigrationManager> migrationManagerFuture;
  private final InjectionFuture<MetricReceiver> metricReceiver;

  /**
   * A map for tracking which migration message (e.g., OwnershipMovedMsg, DataMovedMsg)
   * has been arrived first for a specific block migration.
   * Maintaining only block id is enough, because a block cannot be chosen by multiple migrations at the same time.
   */
  private final Set<Integer> migrationMsgArrivedBlockIds = Collections.synchronizedSet(new HashSet<>());

  @Inject
  private MessageHandlerImpl(final InjectionFuture<TableControlAgent> tableInitializerFuture,
                             final InjectionFuture<MigrationManager> migrationManagerFuture,
                             final InjectionFuture<MetricReceiver> metricReceiver) {
    this.metricReceiver = metricReceiver;
    this.tableInitializerFuture = tableInitializerFuture;
    this.migrationManagerFuture = migrationManagerFuture;
  }

  @Override
  public void onNext(final Message<ETMsg> msg) {
    final ETMsg innerMsg = SingleMessageExtractor.extract(msg);
    switch (innerMsg.getType()) {
    case TableControlMsg:
      onTableControlMsg(innerMsg.getTableControlMsg());
      break;

    case MigrationMsg:
      onMigrationMsg(innerMsg.getMigrationMsg());
      break;

    case MetricMsg:
      onMetricMsg(msg.getSrcId().toString(), innerMsg.getMetricMsg());
      break;

    default:
      throw new RuntimeException("Unexpected message: " + msg);
    }
  }

  private void onMetricMsg(final String srcId, final MetricMsg metricMsg) {
    metricReceiver.get().onMetricMsg(srcId, metricMsg);
  }

  private void onTableControlMsg(final TableControlMsg msg) {
    final Long opId = msg.getOperationId();
    switch (msg.getType()) {
    case TableInitAckMsg:
      onTableInitAckMsg(opId, msg.getTableInitAckMsg());
      break;
    case TableDropAckMsg:
      onTableDropAckMsg(opId, msg.getTableDropAckMsg());
      break;

    default:
      throw new RuntimeException("Unexpected message: " + msg);
    }
  }

  private void onTableInitAckMsg(final long opId, final TableInitAckMsg msg) {
    tableInitializerFuture.get().onTableInitAck(opId, msg.getTableId(), msg.getExecutorId());
  }

  private void onTableDropAckMsg(final long opId, final TableDropAckMsg msg) {
    tableInitializerFuture.get().onTableDropAck(opId, msg.getTableId(), msg.getExecutorId());
  }

  /**
   * Handles MigrationMsgs (e.g., OwnershipAckMsg and DataMovedMsg).
   * @param msg a migration msg
   */
  private void onMigrationMsg(final MigrationMsg msg) {
    final long opId = msg.getOperationId();
    final int blockId;
    final boolean ownershipMovedMsgArrivedFirst;

    switch (msg.getType()) {
    case OwnershipMovedMsg:
      blockId = msg.getOwnershipMovedMsg().getBlockId();
      // Handles the OwnershipAckMsg from the sender that reports an update of a block's ownership.
      migrationManagerFuture.get().ownershipMoved(opId, blockId);

      synchronized (this) {
        if (migrationMsgArrivedBlockIds.contains(blockId)) {
          migrationMsgArrivedBlockIds.remove(blockId);
          ownershipMovedMsgArrivedFirst = false;
        } else {
          migrationMsgArrivedBlockIds.add(blockId);
          ownershipMovedMsgArrivedFirst = true;
        }
      }

      if (!ownershipMovedMsgArrivedFirst) {
        // Handles DataMovedMsg from the sender that reports data migration for a block has been finished successfully.
        migrationManagerFuture.get().markBlockAsMoved(opId, blockId);
      }
      break;

    case DataMovedMsg:
      blockId = msg.getDataMovedMsg().getBlockId();

      synchronized (this) {
        if (migrationMsgArrivedBlockIds.contains(blockId)) {
          migrationMsgArrivedBlockIds.remove(blockId);
          ownershipMovedMsgArrivedFirst = true;
        } else {
          migrationMsgArrivedBlockIds.add(blockId);
          ownershipMovedMsgArrivedFirst = false;
        }
      }

      if (ownershipMovedMsgArrivedFirst) {
        // Handles DataMovedMsg from the sender that reports data migration for a block has been finished successfully.
        migrationManagerFuture.get().markBlockAsMoved(opId, blockId);
      }
      break;

    default:
      throw new RuntimeException("Unexpected message: " + msg);
    }
  }
}
