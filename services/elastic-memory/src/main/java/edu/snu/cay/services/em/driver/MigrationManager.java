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
 * Manages ongoing Migrations.
 */
@DriverSide
public final class MigrationManager {
  private final InjectionFuture<ElasticMemoryMsgSender> sender;
  private final PartitionManager partitionManager;

  private final ConcurrentHashMap<String, MigrationInfo> ongoingMigrations = new ConcurrentHashMap<>();
  private final Set<String> updateWaiters = new HashSet<>();
  private CountDownLatch updateCounter = new CountDownLatch(0);

  private static final Logger LOG = Logger.getLogger(MigrationManager.class.getName());
  private static final String APPLY_UPDATES = "apply_updates";
  private static final String UPDATE_PARTITION = "update_partition";
  private static final int UPDATE_TIMEOUT_MS = 100000; // Wait at most 100 to apply updates.

  @Inject
  private MigrationManager(final InjectionFuture<ElasticMemoryMsgSender> sender,
                           final PartitionManager partitionManager) {
    this.sender = sender;
    this.partitionManager = partitionManager;
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
  public void startMigration(final String operationId,
                             final String senderId,
                             final String receiverId,
                             final String dataType,
                             final Set<LongRange> ranges,
                             final TraceInfo traceInfo) {
    ongoingMigrations.putIfAbsent(operationId, new MigrationInfo(senderId, receiverId, dataType));
    sender.get().sendCtrlMsg(senderId, dataType, receiverId, ranges, operationId, traceInfo);
  }

  /**
   * Register a migration info and send the message to the receiver.
   * @param operationId Identifier of the {@code move} operation.
   * @param senderId Identifier of the sender.
   * @param receiverId Identifier of the receiver.
   * @param dataType Type of the data.
   * @param numUnits Number of units.
   * @param traceInfo Information for Trace.
   */
  public void startMigration(final String operationId,
                             final String senderId,
                             final String receiverId,
                             final String dataType,
                             final int numUnits,
                             final TraceInfo traceInfo) {
    ongoingMigrations.putIfAbsent(operationId, new MigrationInfo(senderId, receiverId, dataType));
    sender.get().sendCtrlMsg(senderId, dataType, receiverId, numUnits, operationId, traceInfo);
  }

  /**
   * Update the range information and make migrations wait
   * until {@link edu.snu.cay.services.em.driver.api.ElasticMemory#applyUpdates()} is called.
   * We will fix this to update the states without this barrier later.
   * @param operationId Identifier of {@code move} operation.
   * @param ranges Ranges of the data.
   */
  public synchronized void waitUpdate(final String operationId, final Set<LongRange> ranges) {
    final MigrationInfo migrationInfo = ongoingMigrations.get(operationId);
    if (migrationInfo == null) {
      throw new IllegalArgumentException("Migration with id : " + operationId + " does not exist.");
    } else {
      checkAndUpdateState(migrationInfo, SENDING_DATA, WAITING_UPDATE);
    }
    migrationInfo.setRanges(ranges); // Update the range information also.

    updateWaiters.add(operationId);
  }

  /**
   * Updates the states of the Driver and Evaluators. Blocks until all updates are applied.
   * @param traceInfo Trace information
   */
  public synchronized void applyUpdates(final TraceInfo traceInfo) {
    try (final TraceScope onApplyUpdate = Trace.startSpan(APPLY_UPDATES, traceInfo)) {
      assert (updateCounter.getCount() == 0); // Make sure there is only one update at a time.
      updateCounter = new CountDownLatch(updateWaiters.size());

      // Update receivers first.
      for (final String updateWaiter : updateWaiters) {
        final MigrationInfo migrationInfo = ongoingMigrations.get(updateWaiter);
        if (migrationInfo == null) {
          throw new IllegalArgumentException("Migration with id : " + updateWaiter + " does not exist.");
        } else {
          checkAndUpdateState(migrationInfo, WAITING_UPDATE, UPDATING_RECEIVER);
        }

        final String receiverId = migrationInfo.getReceiverId();
        sender.get().sendUpdateMsg(receiverId, updateWaiter, traceInfo);
      }

      // This thread holds until all the updates are applied.
      updateCounter.await(UPDATE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
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
    try (final TraceScope updatePartition = Trace.startSpan(UPDATE_PARTITION, traceInfo)) {
      final MigrationInfo migrationInfo = ongoingMigrations.get(operationId);
      if (migrationInfo == null) {
        throw new IllegalArgumentException("Migration with id : " + operationId + " does not exist.");
      } else {
        checkAndUpdateState(migrationInfo, UPDATING_RECEIVER, PARTITION_UPDATED);
      }

      final String senderId = migrationInfo.getSenderId();
      final String receiverId = migrationInfo.getReceiverId();
      final String dataType = migrationInfo.getDataType();

      final Collection<LongRange> ranges = migrationInfo.getRanges();
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
    final MigrationInfo migrationInfo = ongoingMigrations.get(operationId);
    if (migrationInfo == null) {
      throw new IllegalArgumentException("Migration with id : " + operationId + " does not exist.");
    } else {
      checkAndUpdateState(migrationInfo, PARTITION_UPDATED, UPDATING_SENDER);
    }

    final String senderId = migrationInfo.getSenderId();
    sender.get().sendUpdateMsg(senderId, operationId, traceInfo);
  }

  /**
   * Finish the migration.
   * @param operationId Identifier of {@code move} operation.
   */
  public void finishMigration(final String operationId) {
    final MigrationInfo migrationInfo = ongoingMigrations.remove(operationId);
    if (migrationInfo == null) {
      throw new IllegalArgumentException("Migration with id : " + operationId + " does not exist.");
    } else {
      checkAndUpdateState(migrationInfo, UPDATING_SENDER, FINISHED);
    }
    updateCounter.countDown();
  }

  /**
   * Check the current status and update it to the expected state.
   */
  private void checkAndUpdateState(final MigrationInfo migrationInfo,
                                   final MigrationInfo.State expectedCurrentState,
                                   final MigrationInfo.State targetState) {
    if (migrationInfo.getState() == expectedCurrentState) {
      migrationInfo.setState(targetState);
    } else {
      throw new IllegalStateException("The state should be " + expectedCurrentState);
    }
  }
}
