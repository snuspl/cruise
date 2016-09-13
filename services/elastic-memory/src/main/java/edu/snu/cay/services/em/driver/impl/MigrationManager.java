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
import edu.snu.cay.services.em.driver.api.EMRoutingTableUpdate;
import edu.snu.cay.services.em.msg.api.ElasticMemoryCallbackRouter;
import edu.snu.cay.services.em.msg.api.ElasticMemoryMsgSender;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;
import org.htrace.*;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages the migrations requested by EM.move(). When user calls EM.move(),
 * EM registers a migration for tracking the information such as Blocks, and sender/receiver MemoryStores.
 * The status is updated when the MemoryStores sends messages for updating the owner of blocks, or
 * notifying the completion of actual data transfer. MigrationManager also handles the notification of data migration,
 * so MemoryStores can update its routing table immediately after the data migration.
 */
@DriverSide
final class MigrationManager {
  private static final Logger LOG = Logger.getLogger(MigrationManager.class.getName());

  private final InjectionFuture<ElasticMemoryMsgSender> sender;
  private final BlockManager blockManager;
  private final ElasticMemoryCallbackRouter callbackRouter;

  /**
   * This is a mapping from operation id to the {@link Migration}
   * which consists of the current state of each migration.
   */
  private final Map<String, Migration> ongoingMigrations = new HashMap<>();

  /**
   * This map is for notifying changes in EM routing tables to clients.
   * It maintains a mapping from a client id to a corresponding callback.
   */
  private Map<String, EventHandler<EMRoutingTableUpdate>> updateCallbacks = new HashMap<>();

  @Inject
  private MigrationManager(final InjectionFuture<ElasticMemoryMsgSender> sender,
                           final BlockManager blockManager,
                           final ElasticMemoryCallbackRouter callbackRouter) {
    this.sender = sender;
    this.blockManager = blockManager;
    this.callbackRouter = callbackRouter;
  }

  /**
   * Start migration of the data, which moves data blocks from the sender to the receiver Evaluator.
   * Note that operationId should be unique.
   * @param operationId Identifier of the {@code move} operation.
   * @param senderId Identifier of the sender.
   * @param receiverId Identifier of the receiver.
   * @param numBlocks Number of blocks to move.
   * @param parentTraceInfo Information for Trace from parent.
   * @param finishedCallback handler to call when move operation is completed, or null if no callback is needed
   */
  synchronized void startMigration(final String operationId,
                                   final String senderId,
                                   final String receiverId,
                                   final int numBlocks,
                                   @Nullable final TraceInfo parentTraceInfo,
                                   @Nullable final EventHandler<AvroElasticMemoryMessage> finishedCallback) {
    Trace.setProcessId(MigrationManager.class.getSimpleName());

    final TraceScope migrationTraceScope = Trace.startSpan(
        String.format("migration. op_id: %s, sender: %s, receiver: %s, num_blocks: %d ",
            operationId, senderId, receiverId, numBlocks),
        parentTraceInfo);

    if (ongoingMigrations.containsKey(operationId)) {
      LOG.log(Level.WARNING, "Failed to register migration with id {0}. Already exists", operationId);
      return;
    }
    callbackRouter.register(operationId, finishedCallback);

    final List<Integer> blocks = blockManager.chooseBlocksToMove(senderId, numBlocks);

    // Check early failure conditions:
    // there is no block to move (maybe all blocks are moving).
    if (blocks.size() == 0) {
      final String reason =
          "There is no block to move in " + senderId + " of type. Requested numBlocks: " + numBlocks;
      notifyFailure(operationId, reason);
      return;
    }

    ongoingMigrations.put(operationId, new Migration(senderId, receiverId, blocks, migrationTraceScope));
    sender.get().sendCtrlMsg(senderId, receiverId, blocks, operationId,
        TraceInfo.fromSpan(migrationTraceScope.getSpan()));
  }

  /**
   * Mark one block as moved.
   * @param operationId Identifier of {@code move} operation.
   * @param blockId Identifier of the moved block.
   * @param parentTraceInfo Information for Trace from parent.
   */
  synchronized void markBlockAsMoved(final String operationId, final int blockId,
                                     @Nullable final TraceInfo parentTraceInfo) {
    final Migration migration = ongoingMigrations.get(operationId);
    if (migration == null) {
      LOG.log(Level.WARNING, "Migration with ID {0} was not registered, or it has already been finished.", operationId);
      return;
    }

    migration.markBlockAsMoved(blockId);
    blockManager.releaseBlockFromMove(blockId);

    final String senderId = migration.getSenderId();
    final String receiverId = migration.getReceiverId();

    notifyUpdateToClients(senderId, receiverId, blockId, parentTraceInfo);

    if (migration.isComplete()) {
      finishMigration(operationId, parentTraceInfo);
      migration.getTraceScope().close();
    }
  }

  /**
   * Finish migration of the data.
   * @param operationId Identifier of {@code move} operation.
   * @param parentTraceInfo Information for Trace from parent.
   */
  private void finishMigration(final String operationId, @Nullable final TraceInfo parentTraceInfo) {
    final Migration migration = ongoingMigrations.remove(operationId);
    notifySuccess(operationId, migration.getBlockIds());
    broadcastSuccess(migration, parentTraceInfo);
  }

  /**
   * Broadcast the result of the migration to all active evaluators,
   * so as to ensure them to work with an up-to-date routing table.
   * It only sends messages to evaluators except the source and destination of the migration,
   * because their routing tables are already updated during the migration.
   */
  private void broadcastSuccess(final Migration migration, @Nullable final TraceInfo parentTraceInfo) {
    Trace.setProcessId(MigrationManager.class.getSimpleName());

    try (final TraceScope broadcastSuccessScope = Trace.startSpan("broadcast_table_update", parentTraceInfo)) {
      final Set<String> activeEvaluatorIds = blockManager.getActiveEvaluators();
      final String senderId = migration.getSenderId();
      final String receiverId = migration.getReceiverId();
      activeEvaluatorIds.remove(senderId);
      activeEvaluatorIds.remove(receiverId);

      final List<Integer> blockIds = migration.getBlockIds();

      LOG.log(Level.FINE, "Broadcast the result of migration to other active evaluators: {0}", activeEvaluatorIds);

      final TraceInfo traceInfo = TraceInfo.fromSpan(broadcastSuccessScope.getSpan());

      for (final String evalId : activeEvaluatorIds) {
        sender.get().sendRoutingTableUpdateMsg(evalId, blockIds, senderId, receiverId, traceInfo);
      }
    }
  }

  /**
   * Notify the update in the routing table to listening clients in granularity of block.
   */
  private synchronized void notifyUpdateToClients(final String senderId, final String receiverId, final int blockId,
                                                  @Nullable final TraceInfo parentTraceInfo) {
    Trace.setProcessId(MigrationManager.class.getSimpleName());

    try (final TraceScope notifyUpdateToClientsScope = Trace.startSpan("notify_update_to_clients", parentTraceInfo)) {
      final int oldOwnerId = blockManager.getMemoryStoreId(senderId);
      final int newOwnerId = blockManager.getMemoryStoreId(receiverId);

      final EMRoutingTableUpdate update =
          new EMRoutingTableUpdateImpl(oldOwnerId, newOwnerId, receiverId, blockId,
              TraceInfo.fromSpan(notifyUpdateToClientsScope.getSpan()));

      for (final EventHandler<EMRoutingTableUpdate> callBack : updateCallbacks.values()) {
        callBack.onNext(update);
      }
    }
  }

  /**
   * Register a callback for listening updates in EM routing table.
   * @param clientId a client id
   * @param updateCallback a callback
   */
  synchronized void registerRoutingTableUpdateCallback(final String clientId,
                                                       final EventHandler<EMRoutingTableUpdate> updateCallback) {
    updateCallbacks.put(clientId, updateCallback);
  }

  /**
   * Deregister a callback for listening updates in EM routing table.
   * @param clientId a client id
   */
  synchronized void deregisterRoutingTableUpdateCallback(final String clientId) {
    updateCallbacks.remove(clientId);
  }

  /**
   * Fail migration, and notify the failure via callback.
   * @param operationId Identifier of {@code move} operation.
   * @param reason a reason for the failure.
   */
  synchronized void failMigration(final String operationId, final String reason) {
    final Migration migration = ongoingMigrations.remove(operationId);
    if (migration == null) {
      LOG.log(Level.WARNING,
          "Failed migration with ID {0} was not registered, or it has already been removed.", operationId);
    }

    notifyFailure(operationId, reason);
  }

  /**
   * Notify failure to the User via callback.
   * There are some non-nullable fields with blank value, which is not necessary for this type of message.
   * TODO #139: Revisit when the avro message structure is changed.
   */
  private synchronized void notifyFailure(final String moveOperationId, final String reason) {
    final ResultMsg resultMsg = ResultMsg.newBuilder()
        .setResult(Result.FAILURE)
        .setMsg(reason)
        .build();
    final AvroElasticMemoryMessage msg = getEMMessage(moveOperationId, resultMsg);
    callbackRouter.onFailed(msg);
  }

  /**
   * Notify success to the User via callback.
   */
  private synchronized void notifySuccess(final String moveOperationId, final List<Integer> blocks) {
    final ResultMsg resultMsg = ResultMsg.newBuilder()
        .setResult(Result.SUCCESS)
        .setBlockIds(blocks)
        .build();
    final AvroElasticMemoryMessage msg = getEMMessage(moveOperationId, resultMsg);
    callbackRouter.onCompleted(msg);
  }

  private static AvroElasticMemoryMessage getEMMessage(final String operationId, final ResultMsg resultMsg) {
    return AvroElasticMemoryMessage.newBuilder()
        .setType(Type.ResultMsg)
        .setResultMsg(resultMsg)
        .setOperationId(operationId)
        .setSrcId("")
        .setDestId("")
        .build();
  }

  /**
   * Updates the owner of the block.
   * @param operationId id of the operation
   * @param blockId id of the block
   * @param oldOwnerId MemoryStore id which was the owner of the block
   * @param newOwnerId MemoryStore id which will be the owner of the block
   * @param traceInfo Trace information used in HTrace
   */
  void updateOwner(final String operationId, final int blockId, final int oldOwnerId, final int newOwnerId,
                   @Nullable final TraceInfo traceInfo) {
    final Migration migrationInfo = ongoingMigrations.get(operationId);
    final String senderId = migrationInfo.getSenderId();
    blockManager.updateOwner(blockId, oldOwnerId, newOwnerId);

    // Send the OwnershipMessage to update the owner in the sender memoryStore
    sender.get().sendOwnershipMsg(Optional.of(senderId), operationId, blockId, oldOwnerId, newOwnerId, traceInfo);
  }
}
