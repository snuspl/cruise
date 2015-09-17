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

import org.apache.commons.lang.math.LongRange;
import org.apache.reef.annotations.audience.DriverSide;

import javax.inject.Inject;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages ongoing Migrations.
 */
@DriverSide
// TODO #96: Let's revisit the thread-safety later.
public final class MigrationManager {
  /**
   * Represents the status of the migration.
   */
  private enum State {
      SENDING, UPDATING_RECEIVER, UPDATING_SENDER
  }

  private final ConcurrentHashMap<String, MigrationInfo> ongoingMigrations = new ConcurrentHashMap<>();

  @Inject
  private MigrationManager() {
  }

  /**
   * Registers a migration with the information.
   * @param operationId Identifier of the {@code move} operation.
   * @param senderId Identifier of the sender
   * @param receiverId Identifier of the receiver
   */
  public void put(final String operationId,
                  final String senderId,
                  final String receiverId,
                  final String dataType,
                  final Collection<LongRange> ranges) {
    ongoingMigrations.putIfAbsent(operationId, new MigrationInfo(senderId, receiverId, dataType, ranges));
  }

  /**
   * Called when the data has been sent to the receiver successfully.
   * @param operationId Identifier of {@code move} operation.
   */
  public void onDataSent(final String operationId) {
    final MigrationInfo migrationInfo = ongoingMigrations.get(operationId);
    assert migrationInfo.state == State.SENDING;
    migrationInfo.setState(State.UPDATING_RECEIVER);
  }

  /**
   * Called when the MemoryStore's state has been updated in the receiver successfully.
   * @param operationId Identifier of {@code move} operation.
   */
  public void onReceiverUpdated(final String operationId) {
    final MigrationInfo migrationInfo = ongoingMigrations.get(operationId);
    assert migrationInfo.state == State.UPDATING_RECEIVER;
    migrationInfo.setState(State.UPDATING_SENDER);
  }

  /**
   * Called when the MemoryStore's state has been updated in the sender successfully.
   * @param operationId Identifier of {@code move} operation.
   */
  public void onSenderUpdated(final String operationId) {
    final MigrationInfo migrationInfo = ongoingMigrations.get(operationId);
    assert migrationInfo.state == State.UPDATING_SENDER;
    ongoingMigrations.remove(operationId);
  }

  /**
   * @param operationId Identifier of {@code move} operation.
   * @return Identifier of whom sends the data.
   */
  public String getSenderId(final String operationId) {
    return ongoingMigrations.get(operationId).senderId;
  }

  /**
   * @param operationId Identifier of {@code move} operation.
   * @return Identifier of whom receives the data.
   */
  public String getReceiverId(final String operationId) {
    return ongoingMigrations.get(operationId).receiverId;
  }

  /**
   * @param operationId Identifier of {@code move} operation.
   * @return Type of the data to move.
   */
  public String getDataType(final CharSequence operationId) {
    return ongoingMigrations.get(operationId).dataType;
  }

  /**
   * @param operationId Identifier of {@code move} operation.
   * @return Ranges of the data to move.
   */
  public Collection<LongRange> getRanges(final String operationId) {
    return ongoingMigrations.get(operationId).ranges;
  }

  /**
   * Inner class that encapsulates a migration.
   */
  private final class MigrationInfo {
    private final String senderId;
    private final String receiverId;
    private final String dataType;
    private final Collection<LongRange> ranges; // TODO #95: Ranges may not be able to set at the first time
    private State state = State.SENDING;

    /**
     * Creates a new MigrationInfo when move() is requested.
     * @param senderId Identifier of the sender.
     * @param receiverId Identifier of the receiver.
     * @param dataType Type of data
     * @param ranges Set of ranges to move
     */
    private MigrationInfo(final String senderId,
                          final String receiverId,
                          final String dataType,
                          final Collection<LongRange> ranges) {
      this.senderId = senderId;
      this.receiverId = receiverId;
      this.dataType = dataType;
      this.ranges = ranges;
    }

    private State getState() {
      return state;
    }

    private void setState(final State state) {
      this.state = state;
    }
  }
}
