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

import edu.snu.cay.services.em.msg.api.ElasticMemoryMsgSender;
import org.apache.commons.lang.math.LongRange;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.htrace.Trace;
import org.htrace.TraceInfo;
import org.htrace.TraceScope;

import javax.inject.Inject;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.cay.services.em.driver.MigrationInfo.State.*;

/**
 * Manages the status of Migrations. EM registers a migration when User called ElasticMemory.move().
 * Most state transition is done internally in EM, except applyUpdates(): the result of the migration
 * (e.g., location of partitions, MemoryStore in Evaluators) are updated only when user requests explicitly.
 */
@DriverSide
public final class MigrationManager {
  private final InjectionFuture<ElasticMemoryMsgSender> sender;
  private final PartitionManager partitionManager;
  private final int updateTimeoutMillis;

  private final ConcurrentHashMap<String, MigrationInfo> ongoingMigrations = new ConcurrentHashMap<>();
  private final Set<String> waiterIds = new HashSet<>();
  private CountDownLatch updateCounter = new CountDownLatch(0);

  private static final Logger LOG = Logger.getLogger(MigrationManager.class.getName());
  private static final String MOVE_PARTITION = "move_partition";

  @Inject
  private MigrationManager(final InjectionFuture<ElasticMemoryMsgSender> sender,
                           final PartitionManager partitionManager,
                           @Parameter(UpdateTimeoutMillis.class) final int updateTimeoutMillis) {
    this.sender = sender;
    this.partitionManager = partitionManager;
    this.updateTimeoutMillis = updateTimeoutMillis;
  }

  /**
   * Register a migration info and send the message to the receiver.
   * @param operationId Identifier of the {@code move} operation.
   * @param senderId Identifier of the sender.
   * @param receiverId Identifier of the receiver.
   * @param dataType Type of the data.
   * @param ranges Range of the data.
   * @param traceInfo Information for Trace.
   */
  // TODO #90: Instead of RuntimeException, handle the exception properly.
  public synchronized void startMigration(final String operationId,
                                          final String senderId,
                                          final String receiverId,
                                          final String dataType,
                                          final Set<LongRange> ranges,
                                          final TraceInfo traceInfo) {
    if (ongoingMigrations.containsKey(operationId)) {
      throw new RuntimeException("Already registered id : " + operationId);
    } else {
      ongoingMigrations.put(operationId, new MigrationInfo(senderId, receiverId, dataType));
      sender.get().sendCtrlMsg(senderId, dataType, receiverId, ranges, operationId, traceInfo);
    }
  }

  /**
   * Register a migration info and send the message to the receiver. Note that the OperationId should be unique.
   * @param operationId Identifier of the {@code move} operation.
   * @param senderId Identifier of the sender.
   * @param receiverId Identifier of the receiver.
   * @param dataType Type of the data.
   * @param numUnits Number of units.
   * @param traceInfo Information for Trace.
   */
  // TODO #90: Instead of RuntimeException, handle the exception properly.
  public synchronized void startMigration(final String operationId,
                                          final String senderId,
                                          final String receiverId,
                                          final String dataType,
                                          final int numUnits,
                                          final TraceInfo traceInfo) {
    if (ongoingMigrations.containsKey(operationId)) {
      throw new RuntimeException("Already registered id : " + operationId);
    } else {
      ongoingMigrations.put(operationId, new MigrationInfo(senderId, receiverId, dataType));
      sender.get().sendCtrlMsg(senderId, dataType, receiverId, numUnits, operationId, traceInfo);
    }
  }

  /**
   * Update the range information and make migrations wait
   * until {@link edu.snu.cay.services.em.driver.api.ElasticMemory#applyUpdates()} is called.
   * We will fix this to update the states without this barrier later.
   * @param operationId Identifier of {@code move} operation.
   * @param ranges Ranges of the data.
   */
  public synchronized void waitUpdate(final String operationId, final Set<LongRange> ranges) {
    final MigrationInfo updatedInfo = checkAndUpdateState(operationId, SENDING_DATA, WAITING_UPDATE);
    updatedInfo.setRanges(ranges); // Update the range information also.

    waiterIds.add(operationId);
  }

  /**
   * Updates the states of the Driver and Evaluators.
   * Note that this method usually takes long (default timeout: 100s) because it blocks until
   * all waiting updates at the moment are complete.
   * @param traceInfo Trace information
   */
  public void applyUpdates(final TraceInfo traceInfo) {
    try {
      synchronized (this) {
        updateCounter = new CountDownLatch(waiterIds.size());

        // Update receivers first.
        for (final String waiterId : waiterIds) {
          final MigrationInfo updatedInfo = checkAndUpdateState(waiterId, WAITING_UPDATE, UPDATING_RECEIVER);
          final String receiverId = updatedInfo.getReceiverId();
          sender.get().sendUpdateMsg(receiverId, waiterId, traceInfo);
        }
        waiterIds.clear();
      }
      // This thread blocks until all the updates are applied.
      updateCounter.await(updateTimeoutMillis, TimeUnit.MILLISECONDS);
    } catch (final InterruptedException e) {
      // TODO #90: Instead of runtime exception, we need to handle the failure and notify the failure via callback.
      new RuntimeException(e);
    }
  }

  /**
   * Move the partition from the Sender to the Receiver.
   * @param operationId Identifier of the {@code move} operation
   * @param traceInfo Trace information
   */
  public synchronized boolean movePartition(final String operationId, final TraceInfo traceInfo) {
    try (final TraceScope movePartition = Trace.startSpan(MOVE_PARTITION, traceInfo)) {
      final MigrationInfo updatedInfo = checkAndUpdateState(operationId, UPDATING_RECEIVER, MOVING_PARTITION);

      final String senderId = updatedInfo.getSenderId();
      final String receiverId = updatedInfo.getReceiverId();
      final String dataType = updatedInfo.getDataType();

      final Collection<LongRange> ranges = updatedInfo.getRanges();
      boolean result = true;
      for (final LongRange range : ranges) {
        if (!partitionManager.move(senderId, receiverId, dataType, range)) {
          LOG.log(Level.SEVERE, "Failed while moving partition in the range of {0}", range);
          result = false;
        }
      }
      return result;
    }
  }

  /**
   * Sends an update message to the Sender of the migration.
   * @param operationId Identifier of {@code move} operation.
   * @param traceInfo Trace information
   */
  public synchronized void updateSender(final String operationId, final TraceInfo traceInfo) {
    final MigrationInfo updatedInfo = checkAndUpdateState(operationId, MOVING_PARTITION, UPDATING_SENDER);

    final String senderId = updatedInfo.getSenderId();
    sender.get().sendUpdateMsg(senderId, operationId, traceInfo);
  }

  /**
   * Finish the migration.
   * @param operationId Identifier of {@code move} operation.
   */
  public synchronized void finishMigration(final String operationId) {
    checkAndUpdateState(operationId, UPDATING_SENDER, FINISHED);
    ongoingMigrations.remove(operationId);
    updateCounter.countDown();
  }

  /**
   * Check the current status and update it.
   * @return Updated migration information.
   */
  // TODO #90: Instead of runtime exception, we need to handle the failure and notify the failure via callback.
  private synchronized MigrationInfo checkAndUpdateState(final String operationId,
                                                         final MigrationInfo.State expectedCurrentState,
                                                         final MigrationInfo.State targetState) {
    if (!ongoingMigrations.containsKey(operationId)) {
      throw new IllegalArgumentException("Migration with id : " + operationId + " does not exist.");
    }

    final MigrationInfo migrationInfo = ongoingMigrations.get(operationId);
    if (migrationInfo.getState() != expectedCurrentState) {
      throw new IllegalStateException(
          new StringBuilder().append("The state should be ").append(expectedCurrentState)
              .append(", but was ").append(migrationInfo.getState())
              .toString());
    } else {
      migrationInfo.setState(targetState);
      return migrationInfo;
    }
  }

  @NamedParameter(doc = "Timeout for updating EM's state (default: 100s).", default_value = "100000")
  public final class UpdateTimeoutMillis implements Name<Integer> {
  }
}
