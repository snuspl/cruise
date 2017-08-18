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
import edu.snu.cay.services.et.driver.api.AllocatedTable;
import edu.snu.cay.services.et.driver.api.MessageSender;
import edu.snu.cay.services.et.driver.api.MetricReceiver;
import edu.snu.cay.services.et.exceptions.TableNotExistException;
import edu.snu.cay.utils.AvroUtils;
import edu.snu.cay.utils.SingleMessageExtractor;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.io.network.Message;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A message handler implementation.
 */
@DriverSide
public final class MessageHandlerImpl implements MessageHandler {
  private static final Logger LOG = Logger.getLogger(MessageHandlerImpl.class.getName());

  private final String driverId;

  private final InjectionFuture<TableControlAgent> tableControlAgentFuture;
  private final InjectionFuture<MigrationManager> migrationManagerFuture;
  private final InjectionFuture<MetricReceiver> metricReceiver;
  private final InjectionFuture<FallbackManager> fallbackManagerFuture;
  private final InjectionFuture<TableManager> tableManagerFuture;
  private final InjectionFuture<MessageSender> messageSenderFuture;

  /**
   * A map for tracking which migration message (e.g., OwnershipMovedMsg, DataMovedMsg)
   * has been arrived first for a specific block migration.
   * Maintaining only block id is enough, because a block cannot be chosen by multiple migrations at the same time.
   */
  private final Set<Integer> migrationMsgArrivedBlockIds = Collections.synchronizedSet(new HashSet<>());

  @Inject
  private MessageHandlerImpl(@Parameter(DriverIdentifier.class) final String driverId,
                             final InjectionFuture<TableControlAgent> tableControlAgentFuture,
                             final InjectionFuture<TableManager> tableManagerFuture,
                             final InjectionFuture<MigrationManager> migrationManagerFuture,
                             final InjectionFuture<MetricReceiver> metricReceiver,
                             final InjectionFuture<MessageSender> messageSenderFuture,
                             final InjectionFuture<FallbackManager> fallbackManagerFuture) {
    this.driverId = driverId;
    this.metricReceiver = metricReceiver;
    this.tableControlAgentFuture = tableControlAgentFuture;
    this.migrationManagerFuture = migrationManagerFuture;
    this.fallbackManagerFuture = fallbackManagerFuture;
    this.tableManagerFuture = tableManagerFuture;
    this.messageSenderFuture = messageSenderFuture;
  }

  @Override
  public void onNext(final Message<ETMsg> msg) {
    final ETMsg etMsg = SingleMessageExtractor.extract(msg);
    switch (etMsg.getType()) {
    case TableControlMsg:
      onTableControlMsg(msg.getSrcId().toString(),
          AvroUtils.fromBytes(etMsg.getInnerMsg().array(), TableControlMsg.class));
      break;

    case MigrationMsg:
      onMigrationMsg(AvroUtils.fromBytes(etMsg.getInnerMsg().array(), MigrationMsg.class));
      break;

    case MetricMsg:
      onMetricMsg(msg.getSrcId().toString(), AvroUtils.fromBytes(etMsg.getInnerMsg().array(), MetricMsg.class));
      break;

    case TableAccessMsg:
      onTableAccessMsg(msg.getSrcId().toString(),
          AvroUtils.fromBytes(etMsg.getInnerMsg().array(), TableAccessMsg.class));
      break;

    default:
      throw new RuntimeException("Unexpected message: " + msg);
    }
  }

  private void onTableAccessMsg(final String srcId, final TableAccessMsg tableAccessMsg) {
    fallbackManagerFuture.get().onTableAccessReqMessage(srcId, tableAccessMsg);
  }

  private void onMetricMsg(final String srcId, final MetricMsg metricMsg) {
    metricReceiver.get().onMetricMsg(srcId, metricMsg);
  }

  private void onTableControlMsg(final String srcId, final TableControlMsg msg) {
    final Long opId = msg.getOperationId();
    switch (msg.getType()) {
    case TableInitAckMsg:
      onTableInitAckMsg(opId, msg.getTableInitAckMsg());
      break;
    case TableLoadAckMsg:
      onTableLoadAckMsg(opId, msg.getTableLoadAckMsg());
      break;
    case TableDropAckMsg:
      onTableDropAckMsg(opId, msg.getTableDropAckMsg());
      break;
    case OwnershipSyncAckMsg:
      onOwnershipSyncAckMsg(opId, msg.getOwnershipSyncAckMsg());
      break;
    case OwnershipReqMsg:
      onOwnershipReqMsg(srcId, msg.getOwnershipReqMsg());
      break;
    default:
      throw new RuntimeException("Unexpected message: " + msg);
    }
  }

  private void onTableInitAckMsg(final long opId, final TableInitAckMsg msg) {
    tableControlAgentFuture.get().onTableInitAck(opId, msg.getTableId(), msg.getExecutorId());
  }

  private void onTableLoadAckMsg(final long opId, final TableLoadAckMsg msg) {
    tableControlAgentFuture.get().onTableLoadAck(opId, msg.getTableId(), msg.getExecutorId());
  }

  private void onTableDropAckMsg(final long opId, final TableDropAckMsg msg) {
    tableControlAgentFuture.get().onTableDropAck(opId, msg.getTableId(), msg.getExecutorId());
  }

  private void onOwnershipSyncAckMsg(final long opId, final OwnershipSyncAckMsg msg) {
    tableControlAgentFuture.get().onOwnershipSyncAck(opId, msg.getTableId(), msg.getDeletedExecutorId());
  }

  private void onOwnershipReqMsg(final String srcId, final OwnershipReqMsg msg) {
    try {
      final AllocatedTable table = tableManagerFuture.get().getAllocatedTable(msg.getTableId());
      final String owner = table.getOwnerId(msg.getBlockId());

      LOG.log(Level.INFO, "Ownership request from {0}. TableId: {1}, BlockId: {2}, Owner: {3}",
          new Object[]{srcId, msg.getTableId(), msg.getBlockId(), owner});

      // It's different from ordinary ownership update.
      // So use driverId for oldOwnerId, then executor can distinguish that it's the response for ownership request.
      final String oldOwnerId = driverId;
      messageSenderFuture.get().sendOwnershipUpdateMsg(srcId, msg.getTableId(), msg.getBlockId(), oldOwnerId, owner);

    } catch (TableNotExistException e) {
      throw new RuntimeException(e);
    }
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

      synchronized (migrationMsgArrivedBlockIds) {
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
        migrationManagerFuture.get().dataMoved(opId, blockId);
      }
      break;

    case DataMovedMsg:
      blockId = msg.getDataMovedMsg().getBlockId();
      final boolean moveDataAndOwnershipTogether = msg.getDataMovedMsg().getMoveOwnershipTogether();

      if (moveDataAndOwnershipTogether) {
        // for immutable tables, ET migrates data and ownership of block together
        migrationManagerFuture.get().dataAndOwnershipMoved(opId, blockId);

      } else {
        synchronized (migrationMsgArrivedBlockIds) {
          if (migrationMsgArrivedBlockIds.contains(blockId)) {
            migrationMsgArrivedBlockIds.remove(blockId);
            ownershipMovedMsgArrivedFirst = true;
          } else {
            migrationMsgArrivedBlockIds.add(blockId);
            ownershipMovedMsgArrivedFirst = false;
          }
        }

        if (ownershipMovedMsgArrivedFirst) {
          // Handles DataMovedMsg from the sender
          // that reports data migration for a block has been finished successfully.
          migrationManagerFuture.get().dataMoved(opId, blockId);
        }
      }
      break;

    default:
      throw new RuntimeException("Unexpected message: " + msg);
    }
  }
}
