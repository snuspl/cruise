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
import edu.snu.cay.services.et.common.impl.CallbackRegistry;
import edu.snu.cay.services.et.common.util.concurrent.ResultFuture;
import edu.snu.cay.services.et.driver.api.AllocatedTable;
import edu.snu.cay.services.et.driver.api.MessageSender;
import edu.snu.cay.services.et.driver.api.MetricReceiver;
import edu.snu.cay.services.et.exceptions.TableNotExistException;
import edu.snu.cay.utils.AvroUtils;
import edu.snu.cay.utils.CatchableExecutors;
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
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A message handler implementation.
 */
@DriverSide
public final class MessageHandlerImpl implements MessageHandler {
  private static final Logger LOG = Logger.getLogger(MessageHandlerImpl.class.getName());

  private static final int NUM_TBL_CTR_MSG_THREADS = 8;
  private static final int NUM_MIGRATION_MSG_THREADS = 8;
  private static final int NUM_METRIC_MSG_THREADS = 4;
  private static final int NUM_TBL_ACS_MSG_THREADS = 8;
  private static final int NUM_CHKP_THREADS = 8;

  private final ExecutorService tableCtrMsgExecutor = CatchableExecutors.newFixedThreadPool(NUM_TBL_CTR_MSG_THREADS);
  private final ExecutorService migrationMsgExecutor = CatchableExecutors.newFixedThreadPool(NUM_MIGRATION_MSG_THREADS);
  private final ExecutorService metricMsgExecutor = CatchableExecutors.newFixedThreadPool(NUM_METRIC_MSG_THREADS);
  private final ExecutorService tableAccessMsgExecutor = CatchableExecutors.newFixedThreadPool(NUM_TBL_ACS_MSG_THREADS);
  private final ExecutorService chkpMsgExecutor = CatchableExecutors.newFixedThreadPool(NUM_CHKP_THREADS);

  private final String driverId;

  private final InjectionFuture<TableControlAgent> tableControlAgentFuture;
  private final InjectionFuture<MigrationManager> migrationManagerFuture;
  private final InjectionFuture<MetricReceiver> metricReceiver;
  private final InjectionFuture<FallbackManager> fallbackManagerFuture;
  private final InjectionFuture<TableManager> tableManagerFuture;
  private final InjectionFuture<MessageSender> messageSenderFuture;
  private final InjectionFuture<ChkpManagerMaster> chkpManagerMasterFuture;
  private final InjectionFuture<CallbackRegistry> callbackRegistryFuture;

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
                             final InjectionFuture<FallbackManager> fallbackManagerFuture,
                             final InjectionFuture<ChkpManagerMaster> chkpManagerMasterFuture,
                             final InjectionFuture<CallbackRegistry> callbackRegistryFuture) {
    this.driverId = driverId;
    this.metricReceiver = metricReceiver;
    this.tableControlAgentFuture = tableControlAgentFuture;
    this.migrationManagerFuture = migrationManagerFuture;
    this.fallbackManagerFuture = fallbackManagerFuture;
    this.tableManagerFuture = tableManagerFuture;
    this.messageSenderFuture = messageSenderFuture;
    this.chkpManagerMasterFuture = chkpManagerMasterFuture;
    this.callbackRegistryFuture = callbackRegistryFuture;
  }

  @Override
  public void onNext(final Message<ETMsg> msg) {
    final ETMsg etMsg = SingleMessageExtractor.extract(msg);
    switch (etMsg.getType()) {
    case TableControlMsg:
      onTableControlMsg(msg.getSrcId().toString(),
          AvroUtils.fromBytes(etMsg.getInnerMsg().array(), TableControlMsg.class));
      break;

    case TableChkpMsg:
      onTableChkpMsg(AvroUtils.fromBytes(etMsg.getInnerMsg().array(), TableChkpMsg.class));
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

    case TaskletMsg:
      onTaskletMsg(msg.getSrcId().toString(), AvroUtils.fromBytes(etMsg.getInnerMsg().array(), TaskletMsg.class));
      break;

    default:
      throw new RuntimeException("Unexpected message: " + msg);
    }
  }

  private void onTableAccessMsg(final String srcId, final TableAccessMsg tableAccessMsg) {
    tableAccessMsgExecutor.submit(() -> fallbackManagerFuture.get().onTableAccessReqMessage(srcId, tableAccessMsg));
  }

  private void onMetricMsg(final String srcId, final MetricMsg metricMsg) {
    metricMsgExecutor.submit(() -> metricReceiver.get().onMetricMsg(srcId, metricMsg));
  }

  private void onTableChkpMsg(final TableChkpMsg chkpMsg) {
    chkpMsgExecutor.submit(() -> {
      switch (chkpMsg.getType()) {
      case ChkpDoneMsg:
        final ChkpDoneMsg chkpDoneMsg = chkpMsg.getChkpDoneMsg();
        chkpManagerMasterFuture.get().chkpDone(chkpMsg.getChkpId(),
            chkpDoneMsg.getExecutorId(), chkpDoneMsg.getBlockIds());
        break;
      case ChkpCommitMsg:
        final ChkpCommitMsg chkpCommitMsg = chkpMsg.getChkpCommitMsg();
        chkpManagerMasterFuture.get().chkpCommited(chkpMsg.getChkpId(), chkpCommitMsg.getExecutorId());
        break;
      case ChkpLoadDoneMsg:
        final ChkpLoadDoneMsg chkpLoadDoneMsg = chkpMsg.getChkpLoadDoneMsg();
        chkpManagerMasterFuture.get().loadDone(chkpMsg.getChkpId(), chkpLoadDoneMsg.getExecutorId());
        break;
      default:
        throw new RuntimeException("Unexpected msg type");
      }
      return;
    });
  }

  private void onTableControlMsg(final String srcId, final TableControlMsg msg) {
    tableCtrMsgExecutor.submit(() -> {
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
      return;
    });
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
    migrationMsgExecutor.submit(() -> {
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
          // Handles DataMovedMsg from the sender that reports data migration for a block has been finished successfully
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
      return;
    });
  }

  private void onTaskletMsg(final String executorId, final TaskletMsg msg) {
    switch (msg.getType()) {
    case TaskletByteMsg:
      break;

    case TaskletStartMsg:
      final TaskletStartMsg taskletStartMsg = msg.getTaskletStartMsg();
      if (taskletStartMsg.getType() == TaskletStartMsgType.Res) {
        final String taskletId = msg.getTaskletId();
        final ResultFuture<TaskletResult> taskResultFuture = new ResultFuture<>();
        callbackRegistryFuture.get().register(TaskletResult.class,
            executorId + taskletId, taskResultFuture::onCompleted);

        final RunningTasklet runningTasklet = new RunningTasklet(executorId, taskletId,
            taskResultFuture, messageSenderFuture.get());
        callbackRegistryFuture.get().onCompleted(RunningTasklet.class, executorId + taskletId, runningTasklet);
      } else {
        throw new RuntimeException();
      }
      break;
    case TaskletStopMsg:
      final TaskletStopMsg taskletStopMsg = msg.getTaskletStopMsg();
      if (taskletStopMsg.getType() == TaskletStopMsgType.Res) {
        final String taskletId = msg.getTaskletId();
        final TaskletResult taskletResult = new TaskletResult(taskletId, taskletStopMsg.getIsSuccess());
        callbackRegistryFuture.get().onCompleted(TaskletResult.class, executorId + taskletId, taskletResult);
      } else {
        throw new RuntimeException();
      }
      break;
    default:
      throw new RuntimeException("Unexpected message: " + msg);
    }
  }
}
