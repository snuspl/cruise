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
import edu.snu.cay.services.em.avro.FailureMsg;
import edu.snu.cay.services.em.avro.Type;
import edu.snu.cay.services.em.driver.parameters.UpdateTimeoutMillis;
import edu.snu.cay.services.em.msg.api.ElasticMemoryCallbackRouter;
import edu.snu.cay.services.em.msg.api.ElasticMemoryMsgSender;
import edu.snu.cay.utils.LongRangeUtils;
import org.apache.commons.lang.math.LongRange;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.htrace.Trace;
import org.htrace.TraceInfo;
import org.htrace.TraceScope;

import javax.inject.Inject;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
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
  private final InjectionFuture<ElasticMemoryMsgSender> sender;
  private final PartitionManager partitionManager;
  private final ElasticMemoryCallbackRouter callbackRouter;
  private final int updateTimeoutMillis;

  /**
   * This is a mapping from operation id to the {@link Migration}
   * which consists of the current state of each migration.
   */
  private final Map<String, Migration> ongoingMigrations = new HashMap<>();

  /**
   * This set consists of the ranges that are currently moving, which is used
   * to determine the move() is achievable or how many units can be moved.
   * {@link TreeSet} is used for efficiency.
   */
  private final TreeSet<LongRange> movingRanges = LongRangeUtils.createEmptyTreeSet();

  /**
   * This consists of the operation ids of which finish the data transfer,
   * and wait for applying their migration results.
   */
  private final Set<String> waiterIds = new HashSet<>();

  /**
   * The multiple migrations could apply their changes at once.
   * This latch is used for waiting until all the updates are complete.
   */
  private CountDownLatch updateCounter = new CountDownLatch(0);

  private static final Logger LOG = Logger.getLogger(MigrationManager.class.getName());
  private static final String MOVE_PARTITION = "move_partition";

  @Inject
  private MigrationManager(final InjectionFuture<ElasticMemoryMsgSender> sender,
                           final PartitionManager partitionManager,
                           final ElasticMemoryCallbackRouter callbackRouter,
                           @Parameter(UpdateTimeoutMillis.class) final int updateTimeoutMillis) {
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

    } else if (!partitionManager.isMovable(senderId, dataType, ranges, movingRanges)) {
      notifyFailure(operationId, "The ranges are not movable.");
      return;

    } else {
      movingRanges.addAll(ranges);
      ongoingMigrations.put(operationId, new Migration(senderId, receiverId, dataType, ranges));
      sender.get().sendCtrlMsg(senderId, dataType, receiverId, ranges, operationId, traceInfo);
    }
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

    } else {
      final Set<LongRange> movableRanges =
          partitionManager.getMovableRanges(senderId, dataType, numUnits, movingRanges);

      // TODO #90: What would we do if there are not enough number of units, not only there is no data to move?
      if (LongRangeUtils.getNumUnits(movableRanges) == 0) {
        notifyFailure(operationId, "There is no range to move");
        return;
      }

      movingRanges.addAll(movableRanges);
      ongoingMigrations.put(operationId, new Migration(senderId, receiverId, dataType, movableRanges));
      sender.get().sendCtrlMsg(senderId, dataType, receiverId, movableRanges, operationId, traceInfo);
    }
  }

  /**
   * Update the range information and make migrations wait
   * until {@link edu.snu.cay.services.em.driver.api.ElasticMemory#applyUpdates()} is called.
   * We will fix this to update the states without this barrier later.
   * @param operationId Identifier of {@code move} operation.
   */
  synchronized void waitUpdate(final String operationId) {
    checkAndUpdateState(operationId, Migration.SENDING_DATA, Migration.WAITING_UPDATE);
    waiterIds.add(operationId);
  }

  /**
   * Updates the states of the Driver and Evaluators.
   * Note that this method usually takes long (default timeout: 100s) because it blocks until
   * all waiting updates at the moment are complete.
   * @param traceInfo Trace information
   */
  public void applyUpdates(final TraceInfo traceInfo) {
    synchronized (this) {
      updateCounter = new CountDownLatch(waiterIds.size());

      // Update receivers first.
      for (final String waiterId : waiterIds) {
        final Migration updatedInfo =
            checkAndUpdateState(waiterId, Migration.WAITING_UPDATE, Migration.UPDATING_RECEIVER);
        final String receiverId = updatedInfo.getReceiverId();
        sender.get().sendUpdateMsg(receiverId, waiterId, traceInfo);
      }
      waiterIds.clear();
    }

    try {
      // Blocks until all the updates are applied.
      updateCounter.await(updateTimeoutMillis, TimeUnit.MILLISECONDS);
    } catch (final InterruptedException e) {
      // TODO #90: Consider how EM deals with this exception
      new RuntimeException(e);
    }
  }

  /**
   * Move the partition from the Sender to the Receiver.
   * @param operationId Identifier of the {@code move} operation
   * @param traceInfo Trace information
   */
  synchronized void movePartition(final String operationId, final TraceInfo traceInfo) {
    try (final TraceScope movePartition = Trace.startSpan(MOVE_PARTITION, traceInfo)) {
      final Migration updatedInfo =
          checkAndUpdateState(operationId, Migration.UPDATING_RECEIVER, Migration.MOVING_PARTITION);

      final String senderId = updatedInfo.getSenderId();
      final String receiverId = updatedInfo.getReceiverId();
      final String dataType = updatedInfo.getDataType();

      final Collection<LongRange> ranges = updatedInfo.getRanges();
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
    final Migration updatedInfo =
        checkAndUpdateState(operationId, Migration.MOVING_PARTITION, Migration.UPDATING_SENDER);

    final String senderId = updatedInfo.getSenderId();
    sender.get().sendUpdateMsg(senderId, operationId, traceInfo);
  }

  /**
   * Finish the migration.
   * @param operationId Identifier of {@code move} operation.
   */
  synchronized void finishMigration(final String operationId) {
    checkAndUpdateState(operationId, Migration.UPDATING_SENDER, Migration.FINISHED);


    ongoingMigrations.remove(operationId);
    updateCounter.countDown();
  }

  /**
   * Check the current status and update it.
   * @return Updated migration information.
   */
  private synchronized Migration checkAndUpdateState(final String operationId,
                                                         final String expectedCurrentState,
                                                         final String targetState) {

    if (!ongoingMigrations.containsKey(operationId)) {
      notifyFailure(operationId, "The operation id is lost. The last message may have arrived twice.");
    }

    final Migration migration = ongoingMigrations.get(operationId);
    // This failure occurs when the wrong type of messages arrive during the migration,
    // so unexpected state transition is requested.
    migration.checkAndUpdate(expectedCurrentState, targetState);
    return migration;
  }

  /**
   * Notify the failure to the User via callback.
   * There are some non-nullable fields with blank value, which is not necessary for this type of message.
   * Since we will change the message structure, they will be fixed accordingly
   * TODO #139: Revisit when the avro message structure is changed.
   */
  synchronized void notifyFailure(final String operationId, final String reason) {
    final FailureMsg failureMsg = FailureMsg.newBuilder()
        .setReason(reason)
        .build();
    final AvroElasticMemoryMessage msg = AvroElasticMemoryMessage.newBuilder()
        .setType(Type.FailureMsg)
        .setFailureMsg(failureMsg)
        .setOperationId(operationId)
        .setSrcId("")
        .setDestId("")
        .build();
    callbackRouter.onFailed(msg);
  }
}
