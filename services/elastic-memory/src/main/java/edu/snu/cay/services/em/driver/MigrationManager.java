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
package edu.snu.cay.services.em.driver;

import edu.snu.cay.services.em.avro.AvroElasticMemoryMessage;
import edu.snu.cay.services.em.avro.AvroLongRange;
import edu.snu.cay.services.em.avro.Result;
import edu.snu.cay.services.em.avro.ResultMsg;
import edu.snu.cay.services.em.avro.Type;
import edu.snu.cay.services.em.driver.parameters.UpdateTimeoutMillis;
import edu.snu.cay.services.em.msg.api.ElasticMemoryCallbackRouter;
import edu.snu.cay.services.em.msg.api.ElasticMemoryMsgSender;
import edu.snu.cay.services.em.utils.AvroUtils;
import edu.snu.cay.utils.LongRangeUtils;
import org.apache.commons.lang.math.LongRange;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.htrace.Trace;
import org.htrace.TraceInfo;
import org.htrace.TraceScope;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
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
public final class MigrationManager {
  private static final Logger LOG = Logger.getLogger(MigrationManager.class.getName());
  private static final String MOVE_PARTITION = "move_partition";

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
   * This set consists of the ranges that are currently moving, which is used
   * to determine the move() is achievable or how many units can be moved.
   */
  private final NavigableSet<LongRange> movingRanges = LongRangeUtils.createEmptyTreeSet();

  /**
   * This consists of the operation ids of which finish the data transfer,
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
   */
  public synchronized void startMigration(final String operationId,
                                          final String senderId,
                                          final String receiverId,
                                          final String dataType,
                                          final Set<LongRange> ranges,
                                          final TraceInfo traceInfo) {
    if (ongoingMigrations.containsKey(operationId)) {
      LOG.log(Level.WARNING, "Failed to register migration with id {0}. Already exists", operationId);
      return;
    }

    if (!partitionManager.checkRanges(senderId, dataType, ranges)) {
      notifyFailure(operationId, "The ranges are not movable.");
      return;
    }

    movingRanges.addAll(ranges);
    ongoingMigrations.put(operationId, new Migration(senderId, receiverId, dataType));
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
   */
  public synchronized void startMigration(final String operationId,
                                          final String senderId,
                                          final String receiverId,
                                          final String dataType,
                                          final int numUnits,
                                          final TraceInfo traceInfo) {
    if (ongoingMigrations.containsKey(operationId)) {
      LOG.log(Level.WARNING, "Failed to register migration with id {0}. Already exists", operationId);
      return;
    }

    if (!partitionManager.checkDataType(senderId, dataType)) {
      notifyFailure(operationId, "There is no range to move");
      return;
    }

    ongoingMigrations.put(operationId, new Migration(senderId, receiverId, dataType));
    sender.get().sendCtrlMsg(senderId, dataType, receiverId, numUnits, operationId, traceInfo);
  }

  /**
   * Set the range information (set of ranges, number of units) after the data has been transferred.
   * @param operationId Identifier of {@code move} operation.
   * @param rangeSet Set of ranges.
   */
  synchronized void setMovedRange(final String operationId, final Set<LongRange> rangeSet) {
    final Migration migration = ongoingMigrations.get(operationId);
    if (migration == null) {
      LOG.log(Level.WARNING, "Migration with ID {0} was not registered, or it has already been finished.", operationId);
      return;
    }
    migration.checkState(Migration.SENDING_DATA);
    migration.setMovedRange(rangeSet);
  }

  /**
   * Update the range information and make migrations wait
   * until {@link edu.snu.cay.services.em.driver.api.ElasticMemory#applyUpdates()} is called.
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
  }

  /**
   * Updates the states of the Driver and Evaluators.
   * Note that this method usually takes long (default timeout: 100s) because it blocks until
   * all waiting updates at the moment are complete.
   * @param traceInfo Trace information
   */
  public void applyUpdates(final TraceInfo traceInfo) {
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
  synchronized void movePartition(final String operationId, final TraceInfo traceInfo) {
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
        movingRanges.remove(range);
      }
    }
  }

  /**
   * Sends an update message to the Sender of the migration.
   * @param operationId Identifier of {@code move} operation.
   * @param traceInfo Trace information
   */
  synchronized void updateSender(final String operationId, final TraceInfo traceInfo) {
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
   * Finish the migration, and notify the success via callback.
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
   * Notify the failure to the User via callback.
   * There are some non-nullable fields with blank value, which is not necessary for this type of message.
   * Since we will change the message structure, they will be fixed accordingly
   * TODO #139: Revisit when the avro message structure is changed.
   */
  synchronized void notifyFailure(final String operationId, final String reason) {
    final ResultMsg resultMsg = ResultMsg.newBuilder()
        .setResult(Result.FAILURE)
        .setMsg(reason)
        .build();
    final AvroElasticMemoryMessage msg = AvroElasticMemoryMessage.newBuilder()
        .setType(Type.ResultMsg)
        .setResultMsg(resultMsg)
        .setOperationId(operationId)
        .setSrcId("")
        .setDestId("")
        .build();
    callbackRouter.onFailed(msg);
  }

  /**
   * Notify the success to the User via callback.
   * TODO #139: Revisit when the avro message structure is changed.
   */
  synchronized void notifySuccess(final String operationId, final Collection<LongRange> ranges) {
    final List<AvroLongRange> avroRanges = new ArrayList<>(ranges.size());
    for (final LongRange range : ranges) {
      avroRanges.add(AvroUtils.toAvroLongRange(range));
    }

    final ResultMsg resultMsg = ResultMsg.newBuilder()
        .setResult(Result.SUCCESS)
        .setIdRange(avroRanges)
        .build();
    final AvroElasticMemoryMessage msg = AvroElasticMemoryMessage.newBuilder()
        .setType(Type.ResultMsg)
        .setResultMsg(resultMsg)
        .setOperationId(operationId)
        .setSrcId("")
        .setDestId("")
        .build();
    callbackRouter.onCompleted(msg);
  }
}
