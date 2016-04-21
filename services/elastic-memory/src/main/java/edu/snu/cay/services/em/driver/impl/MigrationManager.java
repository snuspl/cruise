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
import edu.snu.cay.services.em.driver.parameters.UpdateTimeoutMillis;
import edu.snu.cay.services.em.msg.api.ElasticMemoryCallbackRouter;
import edu.snu.cay.services.em.msg.api.ElasticMemoryMsgSender;
import edu.snu.cay.services.em.utils.AvroUtils;
import org.apache.commons.lang.math.LongRange;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;
import org.htrace.Trace;
import org.htrace.TraceInfo;
import org.htrace.TraceScope;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages the status of Migrations. EM registers a migration when User called ElasticMemory.move().
 * Most state transition is done internally in EM, except applyUpdates(): the result of the migration
 * (e.g., location of partitions, MemoryStore in Evaluators) are updated only when user requests explicitly.
 *
 * The state of a migration changes as following:
 *  When move() is called, the migration starts with sending the data from Sender to Receiver (SENDING_DATA).
 *  If the data transfer finishes successfully, wait until applyUpdates() is called (WAITING_UPDATE).
 *  When user requests applyUpdates() in EM, update the Receiver first (UPDATING_RECEIVER).
 *  Now we can make sure the data is secure in the Receiver, so partition info can be updated (MOVING_PARTITION).
 *  Next the Sender is updated (UPDATING_SENDER), deleting the temporary data from its MemoryStore.
 *  Once the Sender notifies the Driver that the update completes, the migration finishes (FINISHED).
 */
@DriverSide
final class MigrationManager {
  private static final Logger LOG = Logger.getLogger(MigrationManager.class.getName());
  private static final String MOVE_PARTITION = "move_partition";
  private static final String TRANSFERRED_SUFFIX = "-transferred";
  private static final String FINISHED_SUFFIX = "-finished";

  private final InjectionFuture<ElasticMemoryMsgSender> sender;
  private final PartitionManager partitionManager;
  private final ElasticMemoryCallbackRouter callbackRouter;
  private final long updateTimeoutMillis;

  /**
   * This is a mapping from operation id to the {@link Migration}
   * which consists of the current state of each migration.
   */
  private final Map<String, Migration> ongoingMigrations = new HashMap<>();

  /**
   * This consists of the operation ids of which have finished the data transfer,
   * and wait for applying their migration results.
   */
  private final Set<String> waitingOperationIds = new HashSet<>();

  /**
   * The multiple migrations could apply their changes at once.
   * This latch is used for waiting until all the updates are complete.
   */
  private CountDownLatch updateCounter = new CountDownLatch(0);

  @Inject
  private MigrationManager(final InjectionFuture<ElasticMemoryMsgSender> sender,
                           final PartitionManager partitionManager,
                           final ElasticMemoryCallbackRouter callbackRouter,
                           @Parameter(UpdateTimeoutMillis.class) final long updateTimeoutMillis) {
    this.sender = sender;
    this.partitionManager = partitionManager;
    this.callbackRouter = callbackRouter;
    this.updateTimeoutMillis = updateTimeoutMillis;
  }

  /**
   * Start migration of the data. If the ranges cannot be moved, notify the failure via callback.
   * Note the operationId should be unique.
   * @param operationId Identifier of the {@code move} operation.
   * @param senderId Identifier of the sender.
   * @param receiverId Identifier of the receiver.
   * @param dataType Type of the data.
   * @param ranges Range of the data.
   * @param traceInfo Information for Trace.
   * @param transferredCallback handler to call when data transfer is completed and the migration is waiting
   *                            for a call to {@link #applyUpdates}, or null if no callback is needed
   * @param finishedCallback handler to call when move operation is completed, or null if no callback is needed
   */
  public synchronized void startMigration(final String operationId,
                                          final String senderId,
                                          final String receiverId,
                                          final String dataType,
                                          final Set<LongRange> ranges,
                                          @Nullable final TraceInfo traceInfo,
                                          @Nullable final EventHandler<AvroElasticMemoryMessage> transferredCallback,
                                          @Nullable final EventHandler<AvroElasticMemoryMessage> finishedCallback) {
    if (ongoingMigrations.containsKey(operationId)) {
      LOG.log(Level.WARNING, "Failed to register migration with id {0}. Already exists", operationId);
      return;
    }

    callbackRouter.register(operationId + TRANSFERRED_SUFFIX, transferredCallback);
    callbackRouter.register(operationId + FINISHED_SUFFIX, finishedCallback);
    ongoingMigrations.put(operationId, new Migration(senderId, receiverId, dataType));

    if (!partitionManager.checkRanges(senderId, dataType, ranges)) {
      final String reason = new StringBuilder()
          .append("The requested ranges are not movable from ").append(senderId)
          .append(" of type ").append(dataType)
          .append(". Requested ranges: ").append(Arrays.toString(ranges.toArray()))
          .toString();
      failMigration(operationId, reason);
      return;
    }

    sender.get().sendCtrlMsg(senderId, dataType, receiverId, ranges, operationId, traceInfo);
  }

  /**
   * Start migration of the data. If there is no range to move, notify the failure via callback.
   * Note the operationId should be unique.
   * @param operationId Identifier of the {@code move} operation.
   * @param senderId Identifier of the sender.
   * @param receiverId Identifier of the receiver.
   * @param dataType Type of the data.
   * @param numUnits Number of units.
   * @param traceInfo Information for Trace.
   * @param transferredCallback handler to call when data transfer is completed and the migration is waiting
   *                            for a call to {@link #applyUpdates}, or null if no callback is needed
   * @param finishedCallback handler to call when move operation is completed, or null if no callback is needed
   */
  public synchronized void startMigration(final String operationId,
                                          final String senderId,
                                          final String receiverId,
                                          final String dataType,
                                          final int numUnits,
                                          @Nullable final TraceInfo traceInfo,
                                          @Nullable final EventHandler<AvroElasticMemoryMessage> transferredCallback,
                                          @Nullable final EventHandler<AvroElasticMemoryMessage> finishedCallback) {
    if (ongoingMigrations.containsKey(operationId)) {
      LOG.log(Level.WARNING, "Failed to register migration with id {0}. Already exists", operationId);
      return;
    }

    callbackRouter.register(operationId + TRANSFERRED_SUFFIX, transferredCallback);
    callbackRouter.register(operationId + FINISHED_SUFFIX, finishedCallback);
    ongoingMigrations.put(operationId, new Migration(senderId, receiverId, dataType));

    if (!partitionManager.checkDataType(senderId, dataType)) {
      final String reason = new StringBuilder()
          .append("No data is movable in ").append(senderId)
          .append(" of type ").append(dataType)
          .append(". Requested numUnits: ").append(numUnits)
          .toString();
      failMigration(operationId, reason);
      return;
    }

    sender.get().sendCtrlMsg(senderId, dataType, receiverId, numUnits, operationId, traceInfo);
  }

  /**
   * Start migration of the data, which moves data blocks from the sender to the receiver Evaluator.
   * Note that operationId should be unique.
   * @param operationId Identifier of the {@code move} operation.
   * @param senderId Identifier of the sender.
   * @param receiverId Identifier of the receiver.
   * @param dataType Type of the data.
   * @param numBlocks Number of blocks to move.
   * @param traceInfo Information for Trace.
   * @param finishedCallback handler to call when move operation is completed, or null if no callback is needed
   */
  synchronized void startMigration(final String operationId,
                                   final String senderId,
                                   final String receiverId,
                                   final String dataType,
                                   final int numBlocks,
                                   @Nullable final TraceInfo traceInfo,
                                   @Nullable final EventHandler<AvroElasticMemoryMessage> finishedCallback) {
    if (ongoingMigrations.containsKey(operationId)) {
      LOG.log(Level.WARNING, "Failed to register migration with id {0}. Already exists", operationId);
      return;
    }

    callbackRouter.register(operationId + FINISHED_SUFFIX, finishedCallback);

    final List<Integer> blocks = partitionManager.chooseBlocksToMove(senderId, numBlocks);

    // Check early failure conditions:
    // there is no block to move (maybe all blocks are moving).
    if (blocks.size() == 0) {
      final String reason =
          "There is no block to move in " + senderId + " of type. Requested numBlocks: " + numBlocks;
      failMigration(operationId, reason);
      return;
    }

    ongoingMigrations.put(operationId, new Migration(senderId, receiverId, dataType, blocks));
    sender.get().sendCtrlMsg(senderId, dataType, receiverId, blocks, operationId, traceInfo);
  }

  /**
   * Set the range information after the data has been transferred. Since Evaluators generate dense ranges
   * when sending DataAckMsg, we can count the number of moved units directly from the ranges.
   * @param operationId Identifier of {@code move} operation.
   * @param rangeSet Set of ranges.
   */
  synchronized void setMovedRanges(final String operationId, final Set<LongRange> rangeSet) {
    final Migration migration = ongoingMigrations.get(operationId);
    if (migration == null) {
      LOG.log(Level.WARNING, "Migration with ID {0} was not registered, or it has already been finished.", operationId);
      return;
    }
    migration.checkState(Migration.SENDING_DATA);
    migration.setMovedRanges(rangeSet);
  }

  /**
   * Make migrations wait until {@link edu.snu.cay.services.em.driver.api.ElasticMemory#applyUpdates()} is called.
   * We will fix this to update the states without this barrier later.
   * @param operationId Identifier of {@code move} operation.
   */
  synchronized void waitUpdate(final String operationId) {
    final Migration migration = ongoingMigrations.get(operationId);
    if (migration == null) {
      LOG.log(Level.WARNING, "Migration with ID {0} was not registered, or it has already been finished.", operationId);
      return;
    }
    migration.checkAndUpdate(Migration.SENDING_DATA, Migration.WAITING_UPDATE);
    waitingOperationIds.add(operationId);
    notifyTransferred(operationId);
  }

  /**
   * Updates the states of the Driver and Evaluators.
   * Note that this method usually takes long (default timeout: 100s) because it blocks until
   * all waiting updates at the moment are complete.
   * Since this method is not thread-safe, concurrent invocation could destroy the updates in progress.
   * But currently it is okay because this method is called only in one synchronized context
   * ({@link edu.snu.cay.services.em.driver.api.ElasticMemory#applyUpdates()}).
   * @param traceInfo Trace information
   */
  public void applyUpdates(@Nullable final TraceInfo traceInfo) {
    synchronized (this) {
      updateCounter = new CountDownLatch(waitingOperationIds.size());

      // Update receivers first.
      for (final String waitingOperationId : waitingOperationIds) {
        final Migration migration = ongoingMigrations.get(waitingOperationId);
        if (migration == null) {
          LOG.log(Level.WARNING,
              "Migration with ID {0} was not registered, or it has already been finished.", waitingOperationId);
          continue;
        }
        migration.checkAndUpdate(Migration.WAITING_UPDATE, Migration.UPDATING_RECEIVER);

        final String receiverId = migration.getReceiverId();
        sender.get().sendUpdateMsg(receiverId, waitingOperationId, traceInfo);
      }
      waitingOperationIds.clear();
    }

    try {
      // Blocks until all the updates are applied.
      updateCounter.await(updateTimeoutMillis, TimeUnit.MILLISECONDS);
    } catch (final InterruptedException e) {
      // TODO #90: Consider how EM deals with this exception
      throw new RuntimeException(e);
    }
  }

  /**
   * Move the partition from the Sender to the Receiver.
   * @param operationId Identifier of the {@code move} operation
   * @param traceInfo Trace information
   */
  synchronized void movePartition(final String operationId,
                                  @Nullable final TraceInfo traceInfo) {
    try (final TraceScope movePartition = Trace.startSpan(MOVE_PARTITION, traceInfo)) {
      final Migration migration = ongoingMigrations.get(operationId);
      if (migration == null) {
        LOG.log(Level.WARNING,
            "Migration with ID {0} was not registered, or it has already been finished.", operationId);
        return;
      }
      migration.checkAndUpdate(Migration.UPDATING_RECEIVER, Migration.MOVING_PARTITION);

      final String senderId = migration.getSenderId();
      final String receiverId = migration.getReceiverId();
      final String dataType = migration.getDataType();

      final Collection<LongRange> ranges = migration.getMovedRanges();
      for (final LongRange range : ranges) {
        partitionManager.move(senderId, receiverId, dataType, range);
      }
    }
  }

  /**
   * Sends an update message to the Sender of the migration.
   * @param operationId Identifier of {@code move} operation.
   * @param traceInfo Trace information
   */
  synchronized void updateSender(final String operationId,
                                 @Nullable final TraceInfo traceInfo) {
    final Migration migration = ongoingMigrations.get(operationId);
    if (migration == null) {
      LOG.log(Level.WARNING, "Migration with ID {0} was not registered, or it has already been finished.", operationId);
      return;
    }

    migration.checkAndUpdate(Migration.MOVING_PARTITION, Migration.UPDATING_SENDER);

    final String senderId = migration.getSenderId();
    sender.get().sendUpdateMsg(senderId, operationId, traceInfo);
  }

  /**
   * Finish migration, and notify the success via callback.
   * @param operationId Identifier of {@code move} operation.
   */
  synchronized void finishMigration(final String operationId) {
    final Migration migration = ongoingMigrations.get(operationId);
    if (migration == null) {
      LOG.log(Level.WARNING, "Migration with ID {0} was not registered, or it has already been finished.", operationId);
      return;
    }

    migration.checkAndUpdate(Migration.UPDATING_SENDER, Migration.FINISHED);

    ongoingMigrations.remove(operationId);
    notifySuccess(operationId, migration.getMovedRanges());
    updateCounter.countDown();
  }

  /**
   * Mark one block as moved.
   * @param operationId Identifier of {@code move} operation.
   * @param blockId Identifier of the moved block.
   */
  synchronized void markBlockAsMoved(final String operationId, final int blockId) {
    final Migration migration = ongoingMigrations.get(operationId);
    if (migration == null) {
      LOG.log(Level.WARNING, "Migration with ID {0} was not registered, or it has already been finished.", operationId);
      return;
    }

    migration.markBlockAsMoved(blockId);
    partitionManager.markBlockAsMoved(blockId);

    if (migration.isComplete()) {
      ongoingMigrations.remove(operationId);
      notifySuccess(operationId, migration.getBlockIds());
    }
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
    if (waitingOperationIds.contains(operationId)) {
      notifyTransferFailure(operationId, reason);
    }
    notifyFailure(operationId, reason);
    updateCounter.countDown();
  }

  /**
   * Notify that data transfer failed to the User via callback.
   * There are some non-nullable fields with blank value, which is not necessary for this type of message.
   * TODO #139: Revisit when the avro message structure is changed.
   */
  private void notifyTransferFailure(final String moveOperationId, final String reason) {
    final ResultMsg resultMsg = ResultMsg.newBuilder()
        .setResult(Result.FAILURE)
        .setMsg(reason)
        .build();
    final AvroElasticMemoryMessage msg = getEMMessage(moveOperationId + TRANSFERRED_SUFFIX, resultMsg);
    callbackRouter.onFailed(msg);
  }

  /**
   * Notify that data transfer completed to the User via callback.
   * There are some non-nullable fields with blank value, which is not necessary for this type of message.
   * TODO #139: Revisit when the avro message structure is changed.
   */
  private void notifyTransferred(final String moveOperationId) {
    final ResultMsg resultMsg = ResultMsg.newBuilder()
        .setResult(Result.SUCCESS)
        .build();
    final AvroElasticMemoryMessage msg = getEMMessage(moveOperationId + TRANSFERRED_SUFFIX, resultMsg);
    callbackRouter.onCompleted(msg);
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
    final AvroElasticMemoryMessage msg = getEMMessage(moveOperationId + FINISHED_SUFFIX, resultMsg);
    callbackRouter.onFailed(msg);
  }

  /**
   * Notify success to the User via callback.
   * There are some non-nullable fields with blank value, which is not necessary for this type of message.
   * TODO #139: Revisit when the avro message structure is changed.
   */
  private synchronized void notifySuccess(final String moveOperationId, final Collection<LongRange> ranges) {
    final List<AvroLongRange> avroRanges = new ArrayList<>(ranges.size());
    for (final LongRange range : ranges) {
      avroRanges.add(AvroUtils.toAvroLongRange(range));
    }

    final ResultMsg resultMsg = ResultMsg.newBuilder()
        .setResult(Result.SUCCESS)
        .setIdRange(avroRanges)
        .build();
    final AvroElasticMemoryMessage msg = getEMMessage(moveOperationId + FINISHED_SUFFIX, resultMsg);
    callbackRouter.onCompleted(msg);
  }

  private synchronized void notifySuccess(final String moveOperationId, final List<Integer> blocks) {
    final ResultMsg resultMsg = ResultMsg.newBuilder()
        .setResult(Result.SUCCESS)
        .setBlockIds(blocks)
        .build();
    final AvroElasticMemoryMessage msg = getEMMessage(moveOperationId + FINISHED_SUFFIX, resultMsg);
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
    final String dataType = migrationInfo.getDataType();
    final String senderId = migrationInfo.getSenderId();
    partitionManager.updateOwner(blockId, oldOwnerId, newOwnerId);

    // Send the OwnershipMessage to update the owner in the sender memoryStore
    sender.get().sendOwnershipMsg(Optional.of(senderId), operationId, dataType, blockId, oldOwnerId, newOwnerId,
        traceInfo);
  }
}
